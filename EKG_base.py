<<<<<<< Updated upstream
from neo4j import GraphDatabase
import time


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
        self.stalling_support = {}
        self.not_df = {}
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
        start_time = time.time()
        current_event_id = self.create_event_corr(timestamp, event_name, event_attributes, event_specific_rule)
        print()
        print(time.time() - start_time)
        print()
        print(f"Sent event to Neo4j: {step_number}")
        # ------------> 11: Q ← GET INTEGRATION QUERIES(R) <---------------- #
        # ------------> 12: for all q ∈ Q do <---------------- #
        # ------------> ...  <---------------- #
        # ------------> 14: end for <---------------- #
        start_time = time.time()
        ambiguity, custom_attributes, resolved_ambiguity = self.create_prob_corrs(event_name, event_attributes, event_specific_rule, current_event_id, self.custom_attributes, self.ambiguous_corr, self.all_events)
        print()
        print(time.time() - start_time)
        print()

        start_time = time.time()
        if ambiguity:
            corrs_to_delete = []
            one_ambiguous = 0
            for amb in ambiguity:
                new_element_id, new_element_start, new_element_end, new_element_type, new_element_properties = amb
                if new_element_properties["probability"] == 0.0:
                    corrs_to_delete.append([int(new_element_id), None, -1])
                elif new_element_properties["probability"] != 1.0:
                    one_ambiguous += 1
                    self.ambiguous_corr[new_element_id] = [new_element_start, new_element_end, new_element_type, new_element_properties]
            if one_ambiguous > 0:
                self.stalling_events.append(f"Stalling_from_{current_event_id}")
            if resolved_ambiguity:
                if len(resolved_ambiguity) > 0:
                    if resolved_ambiguity[0] == "NEED DF":
                        new_dfs = self.create_df(None, resolved_ambiguity[1], "NEED DF", resolved_ambiguity[2:])
                        if new_dfs:
                            for new_df in new_dfs:
                                rs_df, rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm = new_df
                                if rs_rxx_has_qm or rs_ryy_has_qm:
                                    rs_df_id = int(rs_df.element_id.split(":")[-1])
                                    self.not_df[rs_df_id] = [rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm]
            df_to_delete = []
            df_to_update = []
            if resolved_ambiguity:
                if len(resolved_ambiguity) > 0:
                    if resolved_ambiguity[0] != "NEED DF":
                        for resolved_corr in resolved_ambiguity:
                            k, v, _ = resolved_corr
                            del self.ambiguous_corr[k]
                            if int(v[1]) in self.stalling_support.keys():
                                prev_support = self.stalling_support[int(v[1])]
                                prev_support.append(v)
                                self.stalling_support[int(v[1])] = prev_support
                            else:
                                self.stalling_support[int(v[1])] = [v]
                            for df_k, df_v in self.not_df.items():
                                if int(k) == df_v[0]:
                                    self.not_df[df_k][1] = v[3]["probability"]
                                    self.not_df[df_k][2] = False
                                elif int(k) == df_v[3]:
                                    self.not_df[df_k][4] = v[3]["probability"]
                                    self.not_df[df_k][5] = False
            for corr_k, corr_v in self.ambiguous_corr.items():
                for df_k, df_v in self.not_df.items():
                    if int(corr_k) == df_v[0]:
                        new_prob = corr_v[3]["probability"]
                        if isinstance(new_prob, str):
                            self.not_df[df_k][1] = float(new_prob[:-1])
                            self.not_df[df_k][2] = True
                        else:
                            self.not_df[df_k][1] = new_prob
                            self.not_df[df_k][2] = False
                    elif int(corr_k) == df_v[3]:
                        new_prob = corr_v[3]["probability"]
                        if isinstance(new_prob, str):
                            self.not_df[df_k][4] = float(new_prob[:-1])
                            self.not_df[df_k][5] = True
                        else:
                            self.not_df[df_k][4] = new_prob
                            self.not_df[df_k][5] = False
            delete_from_dict = []
            for df_k, df_v in self.not_df.items():
                if df_v[2]==False and df_v[5]==False:
                    new_prob = self.not_df[df_k][1] * self.not_df[df_k][4]
                    delete_from_dict.append(df_k)
                    if new_prob == 0.0:
                        df_to_delete.append([df_k, None, -1])
                    elif new_prob == 1.0:
                        df_to_update.append([df_k, df_v, new_prob])
                    else:
                        raise "Safeguarding new probability calculation (impossible in resolved_ambiguity)"
                else:
                    new_prob = str(self.not_df[df_k][1] * self.not_df[df_k][4]) + "?"
                    df_to_update.append([df_k, df_v, new_prob])
            for df_k in delete_from_dict:
                del self.not_df[df_k]

            to_update = []
            if len(corrs_to_delete) > 0:
                to_update.extend(corrs_to_delete)
            if len(df_to_delete) > 0:
                to_update.extend(df_to_delete)
            if len(df_to_update) > 0:
                to_update.extend(df_to_update)
            if len(to_update) > 0:
                self.update_ambiguous(to_update)

        if custom_attributes:
            for k, v in custom_attributes.items():
                self.custom_attributes[k] = v
        print()
        print(time.time() - start_time)
        print()
        # ------------> 15: INFER DF RELATIONSHIPS(G) <---------------- #
        ###########################################################
        start_time = time.time()
        new_dfs = self.create_df(event_name, event_attributes, event_specific_rule, current_event_id)
        if new_dfs:
            for new_df in new_dfs:
                rs_df, rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm = new_df
                if rs_rxx_has_qm or rs_ryy_has_qm:
                    rs_df_id = int(rs_df.element_id.split(":")[-1])
                    self.not_df[rs_df_id] = [rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm]
        print()
        print(time.time() - start_time)
        print()
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
        for e in self.stalling_events[:]:
            if not isinstance(e, str):
                self.stalling_events.remove(e)
                retrieved_event = e
                if event_specific_rule:
                    get_specific_rule = event_specific_rule.get_rule("stalled_printing_support")
                    if not get_specific_rule:
                        raise f"Missing rule for stalled_printing_support"
                    retrieved_event = get_specific_rule(e, self.all_events, self.custom_attributes, self.stalling_support)
                print()
                print("SENDING:            ", retrieved_event)
                # with open("results/disambiguatedStream_runX.txt", "a") as file:
                #    file.write(str(retrieved_event) + "\n")
                print()
            else:
                found_one = False
                for amb_corr_check_k, amb_corr_check_v in self.ambiguous_corr.items():
                    extracted_id = e.split("_")[-1]
                    if extracted_id == amb_corr_check_v[1]:
                        found_one = True
                if found_one:
                    break
                else:
                    self.stalling_events.remove(e)
        print("Stalling events", self.stalling_events)
        # ------------> 18: not directly followed ← GET NOT DF(G) <---------------- #
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

    def update_ambiguous(self, to_update):
        with self.driver.session() as session:
            session.execute_write(self._update_ambiguous, to_update)

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
        else:
            return False, False, False

    @staticmethod
    def _update_ambiguous(tx, to_update):
        update_keys = [x[0] for x in to_update]
        update_probs = [x[2] for x in to_update]
        cypher_query = f"""UNWIND $updates AS update
                                        MATCH ()-[r]->()
                                        WHERE id(r) = update.id
                                        SET r.probability = update.probability
                                        WITH r
                                        WHERE r.probability = -1
                                        DELETE r"""
        params = {"updates": [{"id": key, "probability": prob} for key, prob in zip(update_keys, update_probs)]    }
        tx.run(cypher_query, **params)


    @staticmethod
    def _create_directly_follows(tx, event_name, event_attributes, event_specific_rule, current_event_id):
        if event_specific_rule == "NEED DF":
            cypher_query = """
                // Step 0: Unwind the list of event IDs to process each one
                UNWIND $event_ids AS current_event_id
                CALL {
                    WITH current_event_id
                    CALL {
                    // Step 1: Identify the new event by id.
                    WITH current_event_id
                    MATCH (new:Event)
                    WHERE id(new) = current_event_id
                    WITH new

                    // Step 2: Find all events with timestamps before the new event.
                    MATCH (event:Event)
                    WHERE event.timestamp < new.timestamp
                    WITH new, collect(event) AS prior_events

                    // Step 3: Find all entities that the new event is CORR to
                    MATCH (new)<-[rxx:CORR]-(entity)
                    WITH new, prior_events, collect(entity) AS correlated_entities, collect(rxx) AS rxx_rels

                    // Step 4: Filter out events that are not CORR to the new-related entities and find the most recent event for each entity.
                    UNWIND correlated_entities AS entity
                    UNWIND rxx_rels AS rxx
                    WITH new, prior_events, entity, rxx
                    WHERE entity = startNode(rxx) and NOT id(entity) IN $not_in_ents
                    MATCH (entity)-[ryy:CORR]->(event)
                    WHERE event IN prior_events
                    WITH new, entity, event, rxx, ryy,
                        toFloat(replace(toString(rxx.probability), '?', '')) AS rxx_prob_float,
                            CASE WHEN toString(rxx.probability) ENDS WITH '?' THEN true ELSE false END AS rxx_has_qm,
                        toFloat(replace(toString(ryy.probability), '?', '')) AS ryy_prob_float,
                            CASE WHEN toString(ryy.probability) ENDS WITH '?' THEN true ELSE false END AS ryy_has_qm,
                        rxx.specialization AS rxx_specialization, ryy.specialization AS ryy_specialization
                    ORDER BY event.timestamp DESC
                    RETURN new, entity, event,
                        id(rxx) AS rxx_id, rxx_prob_float, rxx_has_qm,
                        id(ryy) AS ryy_id, ryy_prob_float, ryy_has_qm,
                        rxx_specialization, ryy_specialization
                   }
                   WITH new, entity, collect({
                            event: event,
                            rxx_id: rxx_id,
                            rxx_prob_float: rxx_prob_float,
                            rxx_has_qm: rxx_has_qm,
                            ryy_id: ryy_id,
                            ryy_prob_float: ryy_prob_float,
                            ryy_has_qm: ryy_has_qm,
                            rxx_specialization: rxx_specialization,
                            ryy_specialization: ryy_specialization,
                            timestamp: event.timestamp
                        }) AS entity_event_pairs
                    WITH new, entity, entity_event_pairs, entity_event_pairs[0].timestamp AS max_timestamp
                    WITH new, entity, [e IN entity_event_pairs WHERE e.timestamp = max_timestamp] AS latest_events

                    // Step 5: Create a DF relationship between the most recent event and the new event, with custom attributes detailing the CORR relationship.
                    UNWIND latest_events AS entity_event_pair
                    WITH new, entity,
                        entity_event_pair.event AS mre,
                        entity_event_pair.rxx_id AS rxx_id,
                        entity_event_pair.rxx_prob_float AS rxx_prob_float,
                        entity_event_pair.rxx_has_qm AS rxx_has_qm,
                        entity_event_pair.ryy_id AS ryy_id,
                        entity_event_pair.ryy_prob_float AS ryy_prob_float,
                        entity_event_pair.ryy_has_qm AS ryy_has_qm,
                        entity_event_pair.rxx_specialization AS rxx_specialization,
                        entity_event_pair.ryy_specialization AS ryy_specialization
                    WITH new, entity, mre, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm, rxx_specialization, ryy_specialization,
                        CASE WHEN rxx_has_qm OR ryy_has_qm THEN toString(rxx_prob_float * ryy_prob_float) + '?' ELSE toString(rxx_prob_float * ryy_prob_float) END AS final_prob
                    MERGE (mre)-[df:DF {type: entity.name, specialization: rxx_specialization + "-" + ryy_specialization, probability: final_prob}]->(new)
                    RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
                }
                RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
                """
            result = tx.run(cypher_query, event_ids=current_event_id, not_in_ents = event_attributes)
        else:
            cypher_query = """
                CALL {
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
                WITH new, prior_events, collect(entity) AS correlated_entities, collect(rxx) AS rxx_rels

                // Step 4: Filter out events that are not CORR to the new-related entities and find the most recent event for each entity.
                UNWIND correlated_entities AS entity
                UNWIND rxx_rels AS rxx
                WITH new, prior_events, entity, rxx
                WHERE entity = startNode(rxx)
                MATCH (entity)-[ryy:CORR]->(event)
                WHERE event IN prior_events
                WITH new, entity, event, rxx, ryy,
                    toFloat(replace(toString(rxx.probability), '?', '')) AS rxx_prob_float,
                        CASE WHEN toString(rxx.probability) ENDS WITH '?' THEN true ELSE false END AS rxx_has_qm,
                    toFloat(replace(toString(ryy.probability), '?', '')) AS ryy_prob_float,
                        CASE WHEN toString(ryy.probability) ENDS WITH '?' THEN true ELSE false END AS ryy_has_qm,
                    rxx.specialization AS rxx_specialization, ryy.specialization AS ryy_specialization
                ORDER BY event.timestamp DESC
                RETURN new, entity, event,
                    id(rxx) AS rxx_id, rxx_prob_float, rxx_has_qm,
                    id(ryy) AS ryy_id, ryy_prob_float, ryy_has_qm,
                    rxx_specialization, ryy_specialization
               }
               WITH new, entity, collect({
                        event: event,
                        rxx_id: rxx_id,
                        rxx_prob_float: rxx_prob_float,
                        rxx_has_qm: rxx_has_qm,
                        ryy_id: ryy_id,
                        ryy_prob_float: ryy_prob_float,
                        ryy_has_qm: ryy_has_qm,
                        rxx_specialization: rxx_specialization,
                        ryy_specialization: ryy_specialization,
                        timestamp: event.timestamp
                    }) AS entity_event_pairs
                WITH new, entity, entity_event_pairs, entity_event_pairs[0].timestamp AS max_timestamp
                WITH new, entity, [e IN entity_event_pairs WHERE e.timestamp = max_timestamp] AS latest_events

                // Step 5: Create a DF relationship between the most recent event and the new event, with custom attributes detailing the CORR relationship.
                UNWIND latest_events AS entity_event_pair
                WITH new, entity,
                    entity_event_pair.event AS mre,
                    entity_event_pair.rxx_id AS rxx_id,
                    entity_event_pair.rxx_prob_float AS rxx_prob_float,
                    entity_event_pair.rxx_has_qm AS rxx_has_qm,
                    entity_event_pair.ryy_id AS ryy_id,
                    entity_event_pair.ryy_prob_float AS ryy_prob_float,
                    entity_event_pair.ryy_has_qm AS ryy_has_qm,
                    entity_event_pair.rxx_specialization AS rxx_specialization,
                    entity_event_pair.ryy_specialization AS ryy_specialization
                WITH new, entity, mre, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm, rxx_specialization, ryy_specialization,
                    CASE WHEN rxx_has_qm OR ryy_has_qm THEN toString(rxx_prob_float * ryy_prob_float) + '?' ELSE toString(rxx_prob_float * ryy_prob_float) END AS final_prob
                MERGE (mre)-[df:DF {type: entity.name, specialization: rxx_specialization + "-" + ryy_specialization, probability: final_prob}]->(new)
                RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
            """
            result = tx.run(cypher_query, current_event_id=current_event_id)
        res = [record.values() for record in result]
        if res:
            return res
=======
from neo4j import GraphDatabase
import time


class BaseEKG:

    ###################################################
    # Main functions
    ###################################################
    def __init__(self, uri, user, password, rules):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.rules = rules
        self.custom_attributes = {}
        self.most_recent_event = None
        self.all_events = {}
        self.stalling_events = []
        self.stalling_support = {}
        self.not_df = {}
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
        start_time = time.time()
        current_event_id = self.create_event_corr(timestamp, event_name, event_attributes, event_specific_rule)
        print()
        print(time.time() - start_time)
        print()
        print(f"Sent event to Neo4j: {step_number}")
        # ------------> 11: Q ← GET INTEGRATION QUERIES(R) <---------------- #
        # ------------> 12: for all q ∈ Q do <---------------- #
        # ------------> ...  <---------------- #
        # ------------> 14: end for <---------------- #
        start_time = time.time()
        ambiguity, custom_attributes, resolved_ambiguity = self.create_prob_corrs(event_name, event_attributes, event_specific_rule, current_event_id, self.custom_attributes, self.ambiguous_corr, self.all_events)
        print()
        print(time.time() - start_time)
        print()

        start_time = time.time()
        if ambiguity:
            corrs_to_delete = []
            one_ambiguous = 0
            for amb in ambiguity:
                new_element_id, new_element_start, new_element_end, new_element_type, new_element_properties = amb
                if new_element_properties["probability"] == 0.0:
                    corrs_to_delete.append([int(new_element_id), None, -1])
                elif new_element_properties["probability"] != 1.0:
                    one_ambiguous += 1
                    self.ambiguous_corr[new_element_id] = [new_element_start, new_element_end, new_element_type, new_element_properties]
            if one_ambiguous > 0:
                self.stalling_events.append(f"Stalling_from_{current_event_id}")
            if resolved_ambiguity:
                if len(resolved_ambiguity) > 0:
                    if resolved_ambiguity[0] == "NEED DF":
                        new_dfs = self.create_df(None, resolved_ambiguity[1], "NEED DF", resolved_ambiguity[2:])
                        if new_dfs:
                            for new_df in new_dfs:
                                rs_df, rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm = new_df
                                if rs_rxx_has_qm or rs_ryy_has_qm:
                                    rs_df_id = int(rs_df.element_id.split(":")[-1])
                                    self.not_df[rs_df_id] = [rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm]
            df_to_delete = []
            df_to_update = []
            if resolved_ambiguity:
                if len(resolved_ambiguity) > 0:
                    if resolved_ambiguity[0] != "NEED DF":
                        for resolved_corr in resolved_ambiguity:
                            k, v, _ = resolved_corr
                            del self.ambiguous_corr[k]
                            if int(v[1]) in self.stalling_support.keys():
                                prev_support = self.stalling_support[int(v[1])]
                                prev_support.append(v)
                                self.stalling_support[int(v[1])] = prev_support
                            else:
                                self.stalling_support[int(v[1])] = [v]
                            for df_k, df_v in self.not_df.items():
                                if int(k) == df_v[0]:
                                    self.not_df[df_k][1] = v[3]["probability"]
                                    self.not_df[df_k][2] = False
                                elif int(k) == df_v[3]:
                                    self.not_df[df_k][4] = v[3]["probability"]
                                    self.not_df[df_k][5] = False
            for corr_k, corr_v in self.ambiguous_corr.items():
                for df_k, df_v in self.not_df.items():
                    if int(corr_k) == df_v[0]:
                        new_prob = corr_v[3]["probability"]
                        if isinstance(new_prob, str):
                            self.not_df[df_k][1] = float(new_prob[:-1])
                            self.not_df[df_k][2] = True
                        else:
                            self.not_df[df_k][1] = new_prob
                            self.not_df[df_k][2] = False
                    elif int(corr_k) == df_v[3]:
                        new_prob = corr_v[3]["probability"]
                        if isinstance(new_prob, str):
                            self.not_df[df_k][4] = float(new_prob[:-1])
                            self.not_df[df_k][5] = True
                        else:
                            self.not_df[df_k][4] = new_prob
                            self.not_df[df_k][5] = False
            delete_from_dict = []
            for df_k, df_v in self.not_df.items():
                if df_v[2]==False and df_v[5]==False:
                    new_prob = self.not_df[df_k][1] * self.not_df[df_k][4]
                    delete_from_dict.append(df_k)
                    if new_prob == 0.0:
                        df_to_delete.append([df_k, None, -1])
                    elif new_prob == 1.0:
                        df_to_update.append([df_k, df_v, new_prob])
                    else:
                        raise "Safeguarding new probability calculation (impossible in resolved_ambiguity)"
                else:
                    new_prob = str(self.not_df[df_k][1] * self.not_df[df_k][4]) + "?"
                    df_to_update.append([df_k, df_v, new_prob])
            for df_k in delete_from_dict:
                del self.not_df[df_k]

            to_update = []
            if len(corrs_to_delete) > 0:
                to_update.extend(corrs_to_delete)
            if len(df_to_delete) > 0:
                to_update.extend(df_to_delete)
            if len(df_to_update) > 0:
                to_update.extend(df_to_update)
            if len(to_update) > 0:
                self.update_ambiguous(to_update)

        if custom_attributes:
            for k, v in custom_attributes.items():
                self.custom_attributes[k] = v
        print()
        print(time.time() - start_time)
        print()
        # ------------> 15: INFER DF RELATIONSHIPS(G) <---------------- #
        ###########################################################
        start_time = time.time()
        new_dfs = self.create_df(event_name, event_attributes, event_specific_rule, current_event_id)
        if new_dfs:
            for new_df in new_dfs:
                rs_df, rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm = new_df
                if rs_rxx_has_qm or rs_ryy_has_qm:
                    rs_df_id = int(rs_df.element_id.split(":")[-1])
                    self.not_df[rs_df_id] = [rs_rxx_id, rs_rxx_prob_float, rs_rxx_has_qm, rs_ryy_id, rs_ryy_prob_float, rs_ryy_has_qm]
        print()
        print(time.time() - start_time)
        print()
        # ------------> custom attributes <---------------- #
        print("Custom attr.", self.custom_attributes)
        # ------------> 16: most recent event ← E <---------------- #
        self.most_recent_event = {current_event_id:event}
        print("Most recent", self.most_recent_event)
        # ------------> 17: all events ← all events + E <---------------- #
        self.all_events[current_event_id] = event
        print("All events", self.all_events)
        self.stalling_events.append(event)
        for e in self.stalling_events[:]:
            if not isinstance(e, str):
                self.stalling_events.remove(e)
                retrieved_event = e
                if event_specific_rule:
                    get_specific_rule = event_specific_rule.get_rule("stalled_printing_support")
                    if not get_specific_rule:
                        raise f"Missing rule for stalled_printing_support"
                    retrieved_event = get_specific_rule(e, self.all_events, self.custom_attributes, self.stalling_support)
                print()
                print("SENDING:            ", retrieved_event)
                # with open("results/disambiguatedStream_runX.txt", "a") as file:
                #    file.write(str(retrieved_event) + "\n")
                print()
            else:
                found_one = False
                for amb_corr_check_k, amb_corr_check_v in self.ambiguous_corr.items():
                    extracted_id = e.split("_")[-1]
                    if extracted_id == amb_corr_check_v[1]:
                        found_one = True
                if found_one:
                    break
                else:
                    self.stalling_events.remove(e)
        print("Stalling events", self.stalling_events)
        # ------------> 18: not directly followed ← GET NOT DF(G) <---------------- #
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

    def update_ambiguous(self, to_update):
        with self.driver.session() as session:
            session.execute_write(self._update_ambiguous, to_update)

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
        else:
            return False, False, False

    @staticmethod
    def _update_ambiguous(tx, to_update):
        update_keys = [x[0] for x in to_update]
        update_probs = [x[2] for x in to_update]
        cypher_query = f"""UNWIND $updates AS update
                                        MATCH ()-[r]->()
                                        WHERE id(r) = update.id
                                        SET r.probability = update.probability
                                        WITH r
                                        WHERE r.probability = -1
                                        DELETE r"""
        params = {"updates": [{"id": key, "probability": prob} for key, prob in zip(update_keys, update_probs)]    }
        tx.run(cypher_query, **params)


    @staticmethod
    def _create_directly_follows(tx, event_name, event_attributes, event_specific_rule, current_event_id):
        if event_specific_rule == "NEED DF":
            cypher_query = """
                // Step 0: Unwind the list of event IDs to process each one
                UNWIND $event_ids AS current_event_id
                CALL {
                    WITH current_event_id
                    CALL {
                    // Step 1: Identify the new event by id.
                    WITH current_event_id
                    MATCH (new:Event)
                    WHERE id(new) = current_event_id
                    WITH new

                    // Step 2: Find all events with timestamps before the new event.
                    MATCH (event:Event)
                    WHERE event.timestamp < new.timestamp
                    WITH new, collect(event) AS prior_events

                    // Step 3: Find all entities that the new event is CORR to
                    MATCH (new)<-[rxx:CORR]-(entity)
                    WITH new, prior_events, collect(entity) AS correlated_entities, collect(rxx) AS rxx_rels

                    // Step 4: Filter out events that are not CORR to the new-related entities and find the most recent event for each entity.
                    UNWIND correlated_entities AS entity
                    UNWIND rxx_rels AS rxx
                    WITH new, prior_events, entity, rxx
                    WHERE entity = startNode(rxx) and NOT id(entity) IN $not_in_ents
                    MATCH (entity)-[ryy:CORR]->(event)
                    WHERE event IN prior_events
                    WITH new, entity, event, rxx, ryy,
                        toFloat(replace(toString(rxx.probability), '?', '')) AS rxx_prob_float,
                            CASE WHEN toString(rxx.probability) ENDS WITH '?' THEN true ELSE false END AS rxx_has_qm,
                        toFloat(replace(toString(ryy.probability), '?', '')) AS ryy_prob_float,
                            CASE WHEN toString(ryy.probability) ENDS WITH '?' THEN true ELSE false END AS ryy_has_qm,
                        rxx.specialization AS rxx_specialization, ryy.specialization AS ryy_specialization
                    ORDER BY event.timestamp DESC
                    RETURN new, entity, event,
                        id(rxx) AS rxx_id, rxx_prob_float, rxx_has_qm,
                        id(ryy) AS ryy_id, ryy_prob_float, ryy_has_qm,
                        rxx_specialization, ryy_specialization
                   }
                   WITH new, entity, collect({
                            event: event,
                            rxx_id: rxx_id,
                            rxx_prob_float: rxx_prob_float,
                            rxx_has_qm: rxx_has_qm,
                            ryy_id: ryy_id,
                            ryy_prob_float: ryy_prob_float,
                            ryy_has_qm: ryy_has_qm,
                            rxx_specialization: rxx_specialization,
                            ryy_specialization: ryy_specialization,
                            timestamp: event.timestamp
                        }) AS entity_event_pairs
                    WITH new, entity, entity_event_pairs, entity_event_pairs[0].timestamp AS max_timestamp
                    WITH new, entity, [e IN entity_event_pairs WHERE e.timestamp = max_timestamp] AS latest_events

                    // Step 5: Create a DF relationship between the most recent event and the new event, with custom attributes detailing the CORR relationship.
                    UNWIND latest_events AS entity_event_pair
                    WITH new, entity,
                        entity_event_pair.event AS mre,
                        entity_event_pair.rxx_id AS rxx_id,
                        entity_event_pair.rxx_prob_float AS rxx_prob_float,
                        entity_event_pair.rxx_has_qm AS rxx_has_qm,
                        entity_event_pair.ryy_id AS ryy_id,
                        entity_event_pair.ryy_prob_float AS ryy_prob_float,
                        entity_event_pair.ryy_has_qm AS ryy_has_qm,
                        entity_event_pair.rxx_specialization AS rxx_specialization,
                        entity_event_pair.ryy_specialization AS ryy_specialization
                    WITH new, entity, mre, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm, rxx_specialization, ryy_specialization,
                        CASE WHEN rxx_has_qm OR ryy_has_qm THEN toString(rxx_prob_float * ryy_prob_float) + '?' ELSE toString(rxx_prob_float * ryy_prob_float) END AS final_prob
                    MERGE (mre)-[df:DF {type: entity.name, specialization: rxx_specialization + "-" + ryy_specialization, probability: final_prob}]->(new)
                    RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
                }
                RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
                """
            result = tx.run(cypher_query, event_ids=current_event_id, not_in_ents = event_attributes)
        else:
            cypher_query = """
                CALL {
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
                WITH new, prior_events, collect(entity) AS correlated_entities, collect(rxx) AS rxx_rels

                // Step 4: Filter out events that are not CORR to the new-related entities and find the most recent event for each entity.
                UNWIND correlated_entities AS entity
                UNWIND rxx_rels AS rxx
                WITH new, prior_events, entity, rxx
                WHERE entity = startNode(rxx)
                MATCH (entity)-[ryy:CORR]->(event)
                WHERE event IN prior_events
                WITH new, entity, event, rxx, ryy,
                    toFloat(replace(toString(rxx.probability), '?', '')) AS rxx_prob_float,
                        CASE WHEN toString(rxx.probability) ENDS WITH '?' THEN true ELSE false END AS rxx_has_qm,
                    toFloat(replace(toString(ryy.probability), '?', '')) AS ryy_prob_float,
                        CASE WHEN toString(ryy.probability) ENDS WITH '?' THEN true ELSE false END AS ryy_has_qm,
                    rxx.specialization AS rxx_specialization, ryy.specialization AS ryy_specialization
                ORDER BY event.timestamp DESC
                RETURN new, entity, event,
                    id(rxx) AS rxx_id, rxx_prob_float, rxx_has_qm,
                    id(ryy) AS ryy_id, ryy_prob_float, ryy_has_qm,
                    rxx_specialization, ryy_specialization
               }
               WITH new, entity, collect({
                        event: event,
                        rxx_id: rxx_id,
                        rxx_prob_float: rxx_prob_float,
                        rxx_has_qm: rxx_has_qm,
                        ryy_id: ryy_id,
                        ryy_prob_float: ryy_prob_float,
                        ryy_has_qm: ryy_has_qm,
                        rxx_specialization: rxx_specialization,
                        ryy_specialization: ryy_specialization,
                        timestamp: event.timestamp
                    }) AS entity_event_pairs
                WITH new, entity, entity_event_pairs, entity_event_pairs[0].timestamp AS max_timestamp
                WITH new, entity, [e IN entity_event_pairs WHERE e.timestamp = max_timestamp] AS latest_events

                // Step 5: Create a DF relationship between the most recent event and the new event, with custom attributes detailing the CORR relationship.
                UNWIND latest_events AS entity_event_pair
                WITH new, entity,
                    entity_event_pair.event AS mre,
                    entity_event_pair.rxx_id AS rxx_id,
                    entity_event_pair.rxx_prob_float AS rxx_prob_float,
                    entity_event_pair.rxx_has_qm AS rxx_has_qm,
                    entity_event_pair.ryy_id AS ryy_id,
                    entity_event_pair.ryy_prob_float AS ryy_prob_float,
                    entity_event_pair.ryy_has_qm AS ryy_has_qm,
                    entity_event_pair.rxx_specialization AS rxx_specialization,
                    entity_event_pair.ryy_specialization AS ryy_specialization
                WITH new, entity, mre, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm, rxx_specialization, ryy_specialization,
                    CASE WHEN rxx_has_qm OR ryy_has_qm THEN toString(rxx_prob_float * ryy_prob_float) + '?' ELSE toString(rxx_prob_float * ryy_prob_float) END AS final_prob
                MERGE (mre)-[df:DF {type: entity.name, specialization: rxx_specialization + "-" + ryy_specialization, probability: final_prob}]->(new)
                RETURN df, rxx_id, rxx_prob_float, rxx_has_qm, ryy_id, ryy_prob_float, ryy_has_qm
            """
            result = tx.run(cypher_query, current_event_id=current_event_id)
        res = [record.values() for record in result]
        if res:
            return res
>>>>>>> Stashed changes
