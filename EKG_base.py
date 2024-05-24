from neo4j import GraphDatabase


class BaseEKG:

    ###################################################
    # Main functions
    ###################################################
    def __init__(self, uri, user, password, rules):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.rules = rules
        self.custom_attributes = {}
        self.most_recent_event = None
        self.all_events_order = []
        self.all_events = {}
        self.stalling_events = []
        self.not_df = []
        self.ambiguous_corr = {}

    def close(self):
        self.driver.close()

    def process_event(self, event):
        step_number, event_name, timestamp, event_attributes = event
        # ------------> 3: R ← GET DOMAIN SPECIFIC RULE(e) <---------------- #
        if self.rules:
            event_specific_rule = self.rules
        else:
            event_specific_rule = False
        # ------------> 4: E ← CREATE EVENT(G, e.act name, e.ts value) <---------------- #
        # ------------> 5: for all (key, val) ∈ e do <---------------- #
        # ------------> ... <---------------- #
        # ------------> 10: end for <---------------- #
        current_event_id = self.create_event_corr(timestamp, event_name, event_attributes, event_specific_rule)
        print(f"Sent event to Neo4j: {step_number}")
        # ------------> 11: Q ← GET INTEGRATION QUERIES(R) <---------------- #
        # ------------> 12: for all q ∈ Q do <---------------- #
        # ------------> ...  <---------------- #
        # ------------> 14: end for <---------------- #
        ambiguity, custom_attributes, resolved_ambiguity = self.create_prob_corrs(event_name, event_attributes, event_specific_rule, current_event_id, self.custom_attributes, self.ambiguous_corr, self.all_events)
        if ambiguity:
            to_delete = []
            one_ambiguous = 0
            for amb in ambiguity:
                new_element_id, new_element_start, new_element_end, new_element_type, new_element_properties = amb
                if new_element_properties["probability"] == 0.0:
                    to_delete.append(int(new_element_id))
                elif new_element_properties["probability"] != 1.0:
                    one_ambiguous += 1
                    self.ambiguous_corr[new_element_id] = [new_element_start, new_element_end, new_element_type, new_element_properties]
            if len(to_delete) > 0:
                self.delete_empty_corrs(to_delete)
            if resolved_ambiguity:
                for resolved in resolved_ambiguity:
                    del self.ambiguous_corr[resolved[0]]
            ############################################# NEED TO FIGURE OUT HOW TO DELETE STALLING EVENTS
            if one_ambiguous > 0:
                self.stalling_events.append(f"Stalling_from_{current_event_id}")
        if custom_attributes:
            for k, v in custom_attributes.items():
                self.custom_attributes[k] = v
        ##### RESOLVED AMBIGUITY
        # ------------> 15: INFER DF RELATIONSHIPS(G) <---------------- #
        ########################################################### ADD SOMETHING TO ADD NEW DF WHEN AMBIGUITY RESOLVED
        new_df = self.create_df(event_name, event_attributes, event_specific_rule, current_event_id)
        if new_df:
            for to_remove in new_df:
                rem = int(to_remove.element_id.split(":")[-1])
                if rem in self.not_df:
                    self.not_df.remove(rem)
        if resolved_ambiguity:
            print("RESOLVED:::::::", resolved_ambiguity)
        # ------------> custom attributes <---------------- #
        print("Custom attr.", self.custom_attributes)
        # ------------> 16: most recent event ← E <---------------- #
        self.most_recent_event = {current_event_id:event}
        print("Most recent", self.most_recent_event)
        # ------------> 17: all events ← all events + E <---------------- #
        self.all_events[current_event_id] = event
        self.all_events_order.append(current_event_id)
        print("All events order", self.all_events_order)
        print("All events", self.all_events)
        self.stalling_events.append(event)
        for e in self.stalling_events:
            if not isinstance(e, str):
                self.stalling_events.remove(e)
                print("SENDING:            ", e)
            else:
                break
        print("Stalling events", self.stalling_events)
        # ------------> 18: not directly followed ← GET NOT DF(G) <---------------- #
        self.not_df.append(current_event_id)
        print("Not df", self.not_df)
        # ------------> 19: ambiguous correlations ← GET AMB CORR(G) <---------------- #
        print("Amb corr", self.ambiguous_corr)


    def create_event_corr(self, timestamp, event_name, event_attributes, event_specific_rule):
        with self.driver.session() as session:
            current_event_id = session.execute_write(self._create_event_corr, timestamp, event_name, event_attributes, event_specific_rule)
            return current_event_id

    def create_prob_corrs(self, event_name, event_attributes, event_specific_rule, current_event_id, custom_attributes, ambiguous_corr, all_events):
        with self.driver.session() as session:
            ambiguity_custom_attr = session.execute_write(self._create_prob_corrs, event_name, event_attributes, event_specific_rule, current_event_id, custom_attributes, ambiguous_corr, all_events)
            return ambiguity_custom_attr

    def delete_empty_corrs(self, to_delete):
        with self.driver.session() as session:
            session.execute_write(self._delete_empty_corrs, to_delete)

    def create_df(self, event_name, event_attributes, event_specific_rule, current_event_id):
        with self.driver.session() as session:
            newly_added_df = session.execute_write(self._create_directly_follows, event_name, event_attributes, event_specific_rule, current_event_id)
            return newly_added_df


    @staticmethod
    def _create_event_corr(tx, timestamp, event_name, event_attributes, event_specific_rule):
        # ------------> 4: E ← CREATE EVENT(G, e.act name, e.ts value) <---------------- #
        cypher_query = """CREATE (e:Event {name: $event_name, timestamp: $timestamp})"""
        params = {"event_name": event_name, "timestamp": timestamp.isoformat()}

        # ------------> 5: for all (key, val) ∈ e do <---------------- #
        for i, key in enumerate(event_attributes):
            # ------------> 6: N ← CREATE ENTITY(G, key, val) <---------------- #
            node_alias = f"node_{i}"
            name_alias = f"name_{i}"
            type_alias =  f"type_{i}"
            cypher_query += f"""MERGE ({node_alias}:Entity {{name: ${name_alias}, type: ${type_alias}}})"""
            # ------------> 7: corr type ← GET CORR TYPE(R, key, val) <---------------- #
            # ------------> 8: corr prob ← GET CORR PROB(R, key, val) <---------------- #
            specialization_alias =  f"specialization_{i}"
            probability_alias = f"probability_{i}"
            params[specialization_alias] = "UNKNOWN"
            params[probability_alias] = 1
            if  event_specific_rule:
                get_specific_rule = event_specific_rule.get_rule(event_name)
                if not get_specific_rule:
                    raise f"Missing rule for {event_name}"
                corr_type, corr_prob = get_specific_rule("corr", key)
                params[specialization_alias] = corr_type
                params[probability_alias] = str(corr_prob)

            # ------------> 9: CREATE CORR(G, E, N, corr type, corr prob) <---------------- #
            cypher_query += f"""CREATE ({node_alias})-[:CORR {{specialization: ${specialization_alias}, probability: ${probability_alias}}}]->(e)"""
            params[name_alias] = event_attributes[key]
            params[type_alias] = key
        # ------------> 10: end for <---------------- #
        # Run the dynamically constructed Cypher query
        cypher_query += "RETURN id(e)"
        result = tx.run(cypher_query, **params)
        return result.single()[0]

    @staticmethod
    def _create_prob_corrs(tx, event_name, event_attributes, event_specific_rule, current_event_id, custom_attributes, ambiguous_corr, all_events):
        # ------------> 11: Q ← GET INTEGRATION QUERIES(R) <---------------- #
        if event_specific_rule:
            get_specific_rule = event_specific_rule.get_rule(event_name)
            if not get_specific_rule:
                raise f"Missing rule for {event_name}"
            # ------------> 12: for all q ∈ Q do <---------------- #
            # ------------> 13: EXECUTE QUERY(G, q) <---------------- #
            print("Using integration rule")
            result = get_specific_rule("integration", [tx, event_name, event_attributes, current_event_id], custom_attributes, ambiguous_corr, all_events)
            print("Integration rule used")
            # ------------> 14: end for <---------------- #
            return result

    @staticmethod
    def _delete_empty_corrs(tx, to_delete):
        cypher_query = f"""MATCH ()-[r]->()
                                    WHERE id(r) IN $delete_ids
                                    DELETE r"""
        params = {"delete_ids": to_delete}
        tx.run(cypher_query, **params)

    @staticmethod
    def _create_directly_follows(tx, event_name, event_attributes, event_specific_rule, current_event_id):
        cypher_query = """
        // Step 1: Identify the new event by id.
        MATCH (new:Event)
        WHERE id(new) = $current_event_id
        WITH new

        // Step 2: Find all events with timestamps before the new event.
        MATCH (event:Event)
        WHERE event.timestamp < new.timestamp
        WITH new, collect(event) AS prior_events

        // Step 3: Find all entities that the new event is CORR to
        MATCH (new)<-[rxx:CORR]-(entity)
        WHERE toFloat(rxx.probability) = 1.0
        WITH new, prior_events, collect(entity) AS correlated_entities

        // Step 4: Filter out events that are not CORR to the new-related entities and find the most recent event for each entity.
        UNWIND correlated_entities AS entity
        MATCH (entity)-[:CORR]->(event)
        WHERE event IN prior_events
        WITH new, entity, event
        ORDER BY event.timestamp DESC
        WITH new, entity, collect(event)[0] AS most_recent_event
        WHERE most_recent_event IS NOT NULL
        WITH new, collect({entity: entity, event: most_recent_event}) AS entity_event_pairs

        // Step 5: Create a DF relationship between the most recent event and the new event, with custom attributes detailing the CORR relationship.
        UNWIND entity_event_pairs AS entity_event_pair
        WITH new, entity_event_pair.entity AS entity, entity_event_pair.event AS mre
        MERGE (mre)-[df:DF {type: entity.name}]->(new)

        RETURN mre
        """
        result = tx.run(cypher_query, current_event_id=current_event_id)
        res = [record.values()[0] for record in result]
        if res:
            return res
