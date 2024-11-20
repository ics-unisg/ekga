from neo4j import GraphDatabase
import time
import datetime


class BaseEKG:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def process_event(self, event):
        #print(f"SENT TO Neo4j: {event}")
        current_event_id = self.create_event_entity_corr(event)
        self.create_df(current_event_id, event)

    def create_event_entity_corr(self, event):
        with self.driver.session() as session:
            current_event_id = session.execute_write(self._create_event_entity_corr, event)
            return current_event_id

    def create_df(self, current_event_id, event):
        with self.driver.session() as session:
            session.execute_write(self._create_df, current_event_id, event)

    @staticmethod
    def _create_event_entity_corr(tx, event):
        e_act_name = event["name"]
        e_time = datetime.datetime.fromisoformat(event["timestamp"]).isoformat()
        e_probability = float(event["probability"])
        e_attributes = event["attributes"]
        # ------------> 3: E ← create event(G, e.act name, e.time, e.probability); <------------
        cypher_query = """CREATE (E:Event {name: $name, time: $time, probability: $probability})"""
        params = {"name": e_act_name, "time": e_time, "probability": e_probability}

        # ------------> 4: for all (role, type, id, prob) ∈ e.attributes do <------------
        for i, attribute in enumerate(e_attributes):
            # ------------> 5: N ← create entity(G, type, id); <------------
            N_alias = f"N_{i}"
            type_alias =  f"type_{i}"
            id_alias = f"id_{i}"
            params[type_alias] = attribute[1]
            params[id_alias] = attribute[2]
            cypher_query += f"""MERGE ({N_alias}:Entity {{type: ${type_alias}, id: ${id_alias}}})"""

            # ------------> 6: R ← create correlation(G, E, N, role, prob); <------------
            role_alias =  f"role_{i}"
            prob_alias = f"prob_{i}"
            params[role_alias] = attribute[0]
            params[prob_alias] = attribute[3]
            cypher_query += f"""CREATE ({N_alias})-[:CORR {{role: ${role_alias}, prob: ${prob_alias}}}]->(E)"""

        # ------------> 13: end for <---------------- #
        # Run the dynamically constructed Cypher query
        cypher_query += "RETURN id(E)"
        result = tx.run(cypher_query, **params)
        return result.single()[0]

    @staticmethod
    def _create_df(tx, current_event_id, event):
        cypher_query = """
            CALL {
            // Step 1: Identify the current event by id.
            MATCH (current_event:Event)
            WHERE id(current_event) = $current_event_id
            WITH current_event

            // Step 2: Find all entities that the current event is CORR to.
            MATCH (entity)-[r_current:CORR]->(current_event)
            WITH current_event, collect(entity) AS correlated_entities_current, collect(r_current) AS r_current_rels

            // Step 3: Find all prior events (time before the current event).
            MATCH (prior_event:Event)
            WHERE prior_event.time < current_event.time
            WITH current_event, correlated_entities_current,  r_current_rels, collect(prior_event) AS prior_events

            // Step 4: Filter out events that are not CORR to the current-related entities and find the most recent event for each entity.
            UNWIND correlated_entities_current AS entity
            UNWIND r_current_rels AS r_current
            WITH current_event, prior_events, entity, r_current
            WHERE entity = startNode(r_current)
            MATCH (entity)-[r_previous:CORR]->(event)
            WHERE event IN prior_events
            WITH current_event, entity, event, r_current, r_previous,
                r_current.prob AS r_current_prob, r_previous.prob AS r_previous_prob,
                r_current.role AS r_current_role, r_previous.role AS r_previous_role
            ORDER BY event.timestamp DESC
            RETURN current_event, entity, event,
                id(r_current) AS r_current_id, r_current_prob, r_current_role,
                id(r_previous) AS r_previous_id, r_previous_prob, r_previous_role
           }
           WITH current_event, entity, collect({
                    event: event,
                    r_current_id: r_current_id,
                    r_current_prob: r_current_prob,
                    r_current_role: r_current_role,
                    r_previous_id: r_previous_id,
                    r_previous_prob: r_previous_prob,
                    r_previous_role: r_previous_role,
                    time: event.time
                }) AS entity_event_pairs
            WITH current_event, entity, entity_event_pairs, entity_event_pairs[0].time AS max_time
            WITH current_event, entity, [e IN entity_event_pairs WHERE e.time = max_time] AS latest_events

            // Step 5: Create a DF relationship between the most recent events and the new event, with custom attributes detailing the CORR relationship.
            UNWIND latest_events AS entity_event_pair
            WITH current_event, current_event.probability AS probability, entity,
                entity_event_pair.event AS mre,
                entity_event_pair.event.probability AS mre_probability,
                entity_event_pair.r_current_id AS r_current_id,
                entity_event_pair.r_current_prob AS r_current_prob,
                entity_event_pair.r_current_role AS r_current_role,
                entity_event_pair.r_previous_id AS r_previous_id,
                entity_event_pair.r_previous_prob AS r_previous_prob,
                entity_event_pair.r_previous_role AS r_previous_role
            WITH current_event, entity, mre, r_current_id, r_current_prob, r_current_role, r_previous_id, r_previous_prob, r_previous_role, r_current_prob * r_previous_prob * probability * mre_probability AS final_prob
            MERGE (mre)-[df:DF {type: entity.id, specialization: r_current_role + "#" + r_previous_role, prob: final_prob}]->(current_event)
        """
        result = tx.run(cypher_query, current_event_id=current_event_id)
