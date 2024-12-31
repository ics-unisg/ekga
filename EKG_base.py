################################################################################################
#                                                                                                                                                                                            #
# Author: Dominik Manuel Buchegger                                                                                                                                #
#                                                                                                                                                                                            #
################################################################################################
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
        e_act = event["name"]
        e_time = datetime.datetime.fromisoformat(event["timestamp"]).isoformat()
        e_p = float(event["probability"])
        e_attributes = event["attributes"]
        # CREATE EVENT
        cypher_query = """CREATE (E:Event {act: $act, time: $time, p: $p})"""
        params = {"act": e_act, "time": e_time, "p": e_p}
        # FOR ALL ATTRIBUTES
        for i, attribute in enumerate(e_attributes):
            # CREATE ENTITY
            N_alias = f"N_{i}"
            type_alias_entity =  f"type_entity_{i}"
            id_alias = f"id_{i}"
            params[type_alias_entity] = attribute[1]
            params[id_alias] = attribute[2]
            cypher_query += f"""MERGE ({N_alias}:Entity {{type: ${type_alias_entity}, id: ${id_alias}}})"""
            # CREATE CORR
            type_alias_corr =  f"type_corr_{i}"
            p_alias = f"p_{i}"
            params[type_alias_corr] = attribute[0]
            params[p_alias] = attribute[3]
            cypher_query += f"""CREATE (E)-[:CORR {{type: ${type_alias_corr}, p: ${p_alias}}}]->({N_alias})"""

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
            MATCH (current_event)-[r_current:CORR]->(entity)
            WITH current_event, collect(entity) AS correlated_entities_current, collect(r_current) AS r_current_rels

            // Step 3: Find all prior events (time before the current event).
            MATCH (prior_event:Event)
            WHERE prior_event.time < current_event.time
            WITH current_event, correlated_entities_current,  r_current_rels, collect(prior_event) AS prior_events

            // Step 4: Filter out events that are not CORR to the current-related entities and find the most recent event for each entity.
            UNWIND correlated_entities_current AS entity
            UNWIND r_current_rels AS r_current
            WITH current_event, prior_events, entity, r_current
            WHERE entity = endNode(r_current)
            MATCH (event)-[r_previous:CORR]->(entity)
            WHERE event IN prior_events
            WITH current_event, entity, event, r_current, r_previous,
                r_current.p AS r_current_p, r_previous.p AS r_previous_p,
                r_current.type AS r_current_type, r_previous.type AS r_previous_type
            ORDER BY event.timestamp DESC
            RETURN current_event, entity, event,
                id(r_current) AS r_current_id, r_current_p, r_current_type,
                id(r_previous) AS r_previous_id, r_previous_p, r_previous_type
           }
           WITH current_event, entity, collect({
                    event: event,
                    r_current_id: r_current_id,
                    r_current_p: r_current_p,
                    r_current_type: r_current_type,
                    r_previous_id: r_previous_id,
                    r_previous_p: r_previous_p,
                    r_previous_type: r_previous_type,
                    time: event.time
                }) AS entity_event_pairs
            WITH current_event, entity, entity_event_pairs, entity_event_pairs[0].time AS max_time
            WITH current_event, entity, [e IN entity_event_pairs WHERE e.time = max_time] AS latest_events

            // Step 5: Create a DF relationship between the most recent events and the new event, with custom attributes detailing the CORR relationship.
            UNWIND latest_events AS entity_event_pair
            WITH current_event, current_event.p AS probability, entity,
                entity_event_pair.event AS mre,
                entity_event_pair.event.p AS mre_probability,
                entity_event_pair.r_current_id AS r_current_id,
                entity_event_pair.r_current_p AS r_current_p,
                entity_event_pair.r_current_type AS r_current_type,
                entity_event_pair.r_previous_id AS r_previous_id,
                entity_event_pair.r_previous_p AS r_previous_p,
                entity_event_pair.r_previous_type AS r_previous_type
            WITH current_event, entity, mre, r_current_id, r_current_p, r_current_type, r_previous_id, r_previous_p, r_previous_type, r_current_p * r_previous_p * probability * mre_probability AS final_p
            MERGE (mre)-[df:DF {ent: entity.id, rel: r_current_type + "#" + r_previous_type, p: final_p}]->(current_event)
        """
        result = tx.run(cypher_query, current_event_id=current_event_id)
