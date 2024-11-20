<<<<<<< Updated upstream
from datetime import datetime
from neo4j import GraphDatabase


class RulesEKG():

    def __init__(self):
        self.rules = {
            "Donor check-in": self.rule_donor_checkin,
            "HCW check-in": self.rule_hcw_checkin,
            "Hand hygiene": self.rule_hand_hygiene,
            "Check blood drawing machine": self.rule_check_machine,
            "Apply tourniquet": self.rule_apply_tourniquet,
            "Disinfect injection site": self.rule_disinfect_injection_site,
            "Perform venipuncture": self.rule_perform_venipuncture,
            "Remove tourniquet": self.rule_remove_tourniquet,
            "Activate blood drawing machine": self.rule_activate_machine,
            "Monitor patient": self.rule_monitor_patient,
            "Stop blood drawing machine": self.rule_stop_machine,
            "Remove needle": self.rule_remove_needle,
            "Donor check-out": self.rule_donor_checkout,
            "Take out samples": self.rule_take_out_samples,
            "HCW check-out": self.rule_hcw_checkout,
            "stalled_printing_support": self.stalled_printing_support
        }

    def get_rule(self, event_name):
        if event_name in self.rules.keys():
            return self.rules[event_name]
        else:
            return False

    def stalled_printing_support(self, e, all_events, custom_attributes, support):
        if e[1] == "Hand hygiene" and len(custom_attributes["active_donors"]) > 0:
            retrieved_id = None
            for k, v in all_events.items():
                if v == e:
                    retrieved_id = k
            if not retrieved_id:
                raise "Safeguarding wrong lookup in stalled printing support"
            retrieved_attr = support[retrieved_id]

            final = False
            print(retrieved_attr)
            for attr in retrieved_attr:
                if attr[3]["probability"] == 1.0 and attr[3]["specialization"] == 'HAS_OBJECT':
                    if not final:
                        final = int(attr[0])
                    else:
                        raise "Stalled printing support: Cannot have multiple Objects with 1.0 probability in disambiguated graph"
            if final:
                e[3]["station"] = custom_attributes["donor_station_mapping"][final]
                return e
            else:
                raise "Stalled printing support failed"
        else:
            return e # "Only HH stalled printing support implemented"


    # DONE
    def rule_donor_checkin(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # Donor D2 checks in at R
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_donor_checkin does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            return_attributes = {}
            keep_corrs = []
            new_added_need_df = []
            tx, event_name, event_attributes, current_event_id = key
            # q1
            cypher_query = f"""MATCH (d:Entity {{type: 'donor'}})-[:CORR]->(e:Event)
                                            WHERE id(e) = $current_event_id
                                            RETURN id(d) AS donor_id"""
            params = {}
            params["current_event_id"] = current_event_id
            result = tx.run(cypher_query, **params)
            current_donor = result.single()[0]
            if "active_donors" in custom_attributes.keys():
                previous_donors = custom_attributes["active_donors"]
                new_donors = list(set(previous_donors + [current_donor]))
                return_attributes["active_donors"] = new_donors
            else:
                return_attributes["active_donors"] = [current_donor]
            n = len(return_attributes["active_donors"])
            corrs_to_create_new = []
            for k, v in ambiguous_corr.items():
                if int(v[0]) in return_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            corrs_to_create_new.append(v[1])
                            params = {}
                            if v[3]["specialization"]=="HAS_OBJECT":
                                params["new_probability"] = str(1/n)+"?"
                                v[3]["probability"] = str(1/n)+"?"
                            elif v[3]["specialization"]=="HAS_OBSERVER":
                                params["new_probability"] = str(1-(1/n))+"?"
                                v[3]["probability"] = str(1-(1/n))+"?"
                            cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                            params["relationship_id"] = int(k)
                            tx.run(cypher_query, **params)
                            keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            corrs_to_create_new = list(set(corrs_to_create_new))
            params = {}
            for newly_creat_corr in corrs_to_create_new:
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBJECT', probability: '{str(1/n)+"?"}'}}]->(event)
                                CREATE (donor)-[corr_2:CORR {{specialization: 'HAS_OBSERVER', probability: '{str(1-(1/n))+"?"}'}}]->(event)
                                RETURN corr_1, corr_2"""
                params["donor_id"] = current_donor
                params["current_event_id"] = int(newly_creat_corr)
                new_added_need_df.append(int(newly_creat_corr))
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            # q2
            if "active_hcws" in custom_attributes.keys():
                active_hcws = custom_attributes["active_hcws"]
            else:
                active_hcws = []
            for h in active_hcws:
                if h in custom_attributes["hcw_id_mapping"]:
                    current_hcw = custom_attributes["hcw_id_mapping"][h]
                else:
                    raise "Safeguarding non-existent mapping but existent hcw (should be impossible)"
                event_ids = []
                for k, v in all_events.items():
                    if v[1] == "HCW check-in" and v[3]["hcw"] == current_hcw:
                        event_ids.append((k,v[2]))
                if len(event_ids) > 0:
                    event_id = max(event_ids, key=lambda item: item[1])[0]
                else:
                    raise "Safeguarding non-existent event for existent hcw (should be impossible)"
                active_donors = return_attributes["active_donors"]
                params = {}
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_DONOR', probability: {1.0}}}]->(event)
                                                RETURN corr_1"""
                params["donor_id"] = current_donor
                params["event_id"] = event_id
                new_added_need_df.append(event_id)
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                    new_element_start, new_element_end,
                                                    new_element_type, new_element_properties])
            # q3
            if "donor_station_mapping" in custom_attributes.keys():
                previous_mapping = custom_attributes["donor_station_mapping"]
                previous_mapping[current_donor] = event_attributes["station"]
                return_attributes["donor_station_mapping"] = previous_mapping
            else:
                return_attributes["donor_station_mapping"] = {current_donor: event_attributes["station"]}

            new_added_need_df = list(set(new_added_need_df))
            new_added_need_df_attr = return_attributes["active_donors"].copy()
            new_added_need_df_attr.remove(current_donor)
            new_added_need_df.insert(0, new_added_need_df_attr)
            new_added_need_df.insert(0, "NEED DF")
            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, new_added_need_df
            else:
                return False, return_attributes, new_added_need_df

    # DONE
    def rule_hcw_checkin(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # HCW H1 checks in at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hcw_checkin does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            return_attributes = {}
            # q1
            if "active_donors" in custom_attributes.keys():
                active_donors = custom_attributes["active_donors"]
            else:
                active_donors = []
            n = len(active_donors)
            keep_corrs = []
            for i, donor_id in enumerate(active_donors):
                params = {}
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                            WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                            CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_DONOR', probability: {1.0}}}]->(event)
                                            RETURN corr_1"""
                params["donor_id"] = donor_id
                params["current_event_id"] = current_event_id
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            # q2
            cypher_query = f"""MATCH (h:Entity {{type: 'hcw'}})-[:CORR]->(e:Event)
                                            WHERE id(e) = $current_event_id
                                            RETURN id(h) AS hcw_id"""
            params = {}
            params["current_event_id"] = current_event_id
            result = tx.run(cypher_query, **params)
            current_hcw = result.single()[0]
            if "active_hcws" in custom_attributes.keys():
                previous_hcws = custom_attributes["active_hcws"]
                new_hcws = list(set(previous_hcws + [current_hcw]))
                return_attributes["active_hcws"] = new_hcws
            else:
                return_attributes["active_hcws"] = [current_hcw]
            # q3
            if "hcw_id_mapping" in custom_attributes.keys():
                previous_mapping = custom_attributes["hcw_id_mapping"]
                previous_mapping[current_hcw] = event_attributes["hcw"]
                return_attributes["hcw_id_mapping"] = previous_mapping
            else:
                return_attributes["hcw_id_mapping"] = {current_hcw: event_attributes["hcw"]}

            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, False
            else:
                return False, return_attributes, False

    # DONE
    def rule_hand_hygiene(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 performs hand hygiene at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hand_hygiene does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            return_attributes = {}
            # q1
            if "active_donors" in custom_attributes.keys():
                active_donors = custom_attributes["active_donors"]
            else:
                active_donors = []
            n = len(active_donors)
            params = {}
            keep_corrs = []
            for i, donor_id in enumerate(active_donors):
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                            WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                            CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBJECT', probability: '{str(1/n)+"?"}'}}]->(event)
                                            CREATE (donor)-[corr_2:CORR {{specialization: 'HAS_OBSERVER', probability: '{str(1-(1/n))+"?"}'}}]->(event)
                                            RETURN corr_1, corr_2"""
                params["donor_id"] = donor_id
                params["current_event_id"] = current_event_id
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, False
            else:
                return False, return_attributes, False

    # DONE
    def rule_check_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 checks blood drawing machine ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_CHECK_TARGET"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_check_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_apply_tourniquet(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 applies tourniquet on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_apply_tourniquet does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_disinfect_injection_site(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 disinfects injection site on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_disinfect_injection_site does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_perform_venipuncture(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 performs venipuncture on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_perform_venipuncture does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_remove_tourniquet(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 removes tourniquet from D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_remove_tourniquet does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_activate_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 activates the blood drawing machine MR at R
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_activate_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_monitor_patient(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 monitors D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_monitor_patient does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_stop_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 stops blood drawing machine ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_stop_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_remove_needle(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 removes needle from D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_remove_needle does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_donor_checkout(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # D1 checks out at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_donor_checkout does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            keep_corrs = []
            resolved_ambiguity = []
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            params = {}
                            if int(v[0]) == this_donor:
                                params["new_probability"] = 0.0
                                v[3]["probability"] = 0.0
                            else:
                                if v[3]["specialization"]=="HAS_OBSERVER":
                                    params["new_probability"] = 0.0
                                    v[3]["probability"] = 0.0
                                elif v[3]["specialization"]=="HAS_OBJECT":
                                    params["new_probability"] = 1.0
                                    v[3]["probability"] = 1.0
                            cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                            params["relationship_id"] = int(k)
                            tx.run(cypher_query, **params)
                            keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                            resolved_ambiguity.append([k, v, None])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            # q2
            return_attributes = {}
            if "donor_station_mapping" in custom_attributes.keys():
                if "active_donors" in custom_attributes.keys():
                    previous_donors = custom_attributes["active_donors"]
                    if this_donor in previous_donors:
                        previous_donors.remove(this_donor)
                    else:
                        raise "Safeguarding previous donors removal"
                    if len(previous_donors) > 0:
                        return_attributes["active_donors"] = previous_donors
                if "donor_station_mapping" in custom_attributes.keys():
                    previous_mapping = custom_attributes["donor_station_mapping"]
                    if this_donor in previous_mapping.keys():
                        del previous_mapping[this_donor]
                    else:
                        raise "Safeguarding previous donor-mapping removal"
                    if len(previous_mapping.keys()) > 0:
                        return_attributes["donor_station_mapping"] = previous_mapping

            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, resolved_ambiguity
            else:
                return False, return_attributes, False

    # DONE
    def rule_take_out_samples(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 takes out the samples from ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_take_out_samples does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            return False, False, False

    # DONE
    def rule_hcw_checkout(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # HCW H1 checks out at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hcw_checkout does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            return_attributes = {}
            if "hcw_id_mapping" in custom_attributes.keys():
                for k, v in custom_attributes["hcw_id_mapping"].items():
                    if v == event_attributes["hcw"]:
                        this_hcw = k
                if "active_hcws" in custom_attributes.keys():
                    previous_hcws = custom_attributes["active_hcws"]
                    if this_hcw in previous_hcws:
                        previous_hcws.remove(this_hcw)
                    else:
                        raise "Safeguarding previous hcws removal"
                    if len(previous_hcws) > 0:
                        return_attributes["active_hcws"] = previous_hcws
                if "hcw_id_mapping" in custom_attributes.keys():
                    previous_mapping = custom_attributes["hcw_id_mapping"]
                    if this_hcw in previous_mapping.keys():
                        del previous_mapping[this_hcw]
                    else:
                        raise "Safeguarding previous hcw-mapping removal"
                    if len(previous_mapping.keys()) > 0:
                        return_attributes["hcw_id_mapping"] = previous_mapping

            return False, return_attributes, False
=======
from datetime import datetime
from neo4j import GraphDatabase


class RulesEKG():

    def __init__(self):
        self.rules = {
            "Donor check-in": self.rule_donor_checkin,
            "HCW check-in": self.rule_hcw_checkin,
            "Hand hygiene": self.rule_hand_hygiene,
            "Check blood drawing machine": self.rule_check_machine,
            "Apply tourniquet": self.rule_apply_tourniquet,
            "Disinfect injection site": self.rule_disinfect_injection_site,
            "Perform venipuncture": self.rule_perform_venipuncture,
            "Remove tourniquet": self.rule_remove_tourniquet,
            "Activate blood drawing machine": self.rule_activate_machine,
            "Monitor patient": self.rule_monitor_patient,
            "Stop blood drawing machine": self.rule_stop_machine,
            "Remove needle": self.rule_remove_needle,
            "Donor check-out": self.rule_donor_checkout,
            "Take out samples": self.rule_take_out_samples,
            "HCW check-out": self.rule_hcw_checkout,
            "stalled_printing_support": self.stalled_printing_support
        }

    def get_rule(self, event_name):
        if event_name in self.rules.keys():
            return self.rules[event_name]
        else:
            return False

    def stalled_printing_support(self, e, all_events, custom_attributes, support):
        if e[1] == "Hand hygiene" and len(custom_attributes["active_donors"]) > 0:
            retrieved_id = None
            for k, v in all_events.items():
                if v == e:
                    retrieved_id = k
            if not retrieved_id:
                raise "Safeguarding wrong lookup in stalled printing support"
            retrieved_attr = support[retrieved_id]

            final = False
            print(retrieved_attr)
            for attr in retrieved_attr:
                if attr[3]["probability"] == 1.0 and attr[3]["specialization"] == 'HAS_OBJECT':
                    if not final:
                        final = int(attr[0])
                    else:
                        raise "Stalled printing support: Cannot have multiple Objects with 1.0 probability in disambiguated graph"
            if final:
                e[3]["station"] = custom_attributes["donor_station_mapping"][final]
                return e
            else:
                raise "Stalled printing support failed"
        else:
            return e # "Only HH stalled printing support implemented"


    # DONE
    def rule_donor_checkin(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # Donor D2 checks in at R
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_donor_checkin does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            return_attributes = {}
            keep_corrs = []
            new_added_need_df = []
            tx, event_name, event_attributes, current_event_id = key
            # q1
            cypher_query = f"""MATCH (d:Entity {{type: 'donor'}})-[:CORR]->(e:Event)
                                            WHERE id(e) = $current_event_id
                                            RETURN id(d) AS donor_id"""
            params = {}
            params["current_event_id"] = current_event_id
            result = tx.run(cypher_query, **params)
            current_donor = result.single()[0]
            if "active_donors" in custom_attributes.keys():
                previous_donors = custom_attributes["active_donors"]
                new_donors = list(set(previous_donors + [current_donor]))
                return_attributes["active_donors"] = new_donors
            else:
                return_attributes["active_donors"] = [current_donor]
            n = len(return_attributes["active_donors"])
            corrs_to_create_new = []
            for k, v in ambiguous_corr.items():
                if int(v[0]) in return_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            corrs_to_create_new.append(v[1])
                            params = {}
                            if v[3]["specialization"]=="HAS_OBJECT":
                                params["new_probability"] = str(1/n)+"?"
                                v[3]["probability"] = str(1/n)+"?"
                            elif v[3]["specialization"]=="HAS_OBSERVER":
                                params["new_probability"] = str(1-(1/n))+"?"
                                v[3]["probability"] = str(1-(1/n))+"?"
                            cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                            params["relationship_id"] = int(k)
                            tx.run(cypher_query, **params)
                            keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            corrs_to_create_new = list(set(corrs_to_create_new))
            params = {}
            for newly_creat_corr in corrs_to_create_new:
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBJECT', probability: '{str(1/n)+"?"}'}}]->(event)
                                CREATE (donor)-[corr_2:CORR {{specialization: 'HAS_OBSERVER', probability: '{str(1-(1/n))+"?"}'}}]->(event)
                                RETURN corr_1, corr_2"""
                params["donor_id"] = current_donor
                params["current_event_id"] = int(newly_creat_corr)
                new_added_need_df.append(int(newly_creat_corr))
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            # q2
            if "active_hcws" in custom_attributes.keys():
                active_hcws = custom_attributes["active_hcws"]
            else:
                active_hcws = []
            for h in active_hcws:
                if h in custom_attributes["hcw_id_mapping"]:
                    current_hcw = custom_attributes["hcw_id_mapping"][h]
                else:
                    raise "Safeguarding non-existent mapping but existent hcw (should be impossible)"
                event_ids = []
                for k, v in all_events.items():
                    if v[1] == "HCW check-in" and v[3]["hcw"] == current_hcw:
                        event_ids.append((k,v[2]))
                if len(event_ids) > 0:
                    event_id = max(event_ids, key=lambda item: item[1])[0]
                else:
                    raise "Safeguarding non-existent event for existent hcw (should be impossible)"
                active_donors = return_attributes["active_donors"]
                params = {}
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_DONOR', probability: {1.0}}}]->(event)
                                                RETURN corr_1"""
                params["donor_id"] = current_donor
                params["event_id"] = event_id
                new_added_need_df.append(event_id)
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                    new_element_start, new_element_end,
                                                    new_element_type, new_element_properties])
            # q3
            if "donor_station_mapping" in custom_attributes.keys():
                previous_mapping = custom_attributes["donor_station_mapping"]
                previous_mapping[current_donor] = event_attributes["station"]
                return_attributes["donor_station_mapping"] = previous_mapping
            else:
                return_attributes["donor_station_mapping"] = {current_donor: event_attributes["station"]}

            new_added_need_df = list(set(new_added_need_df))
            new_added_need_df_attr = return_attributes["active_donors"].copy()
            new_added_need_df_attr.remove(current_donor)
            new_added_need_df.insert(0, new_added_need_df_attr)
            new_added_need_df.insert(0, "NEED DF")
            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, new_added_need_df
            else:
                return False, return_attributes, new_added_need_df

    # DONE
    def rule_hcw_checkin(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # HCW H1 checks in at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hcw_checkin does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            return_attributes = {}
            # q1
            if "active_donors" in custom_attributes.keys():
                active_donors = custom_attributes["active_donors"]
            else:
                active_donors = []
            n = len(active_donors)
            keep_corrs = []
            for i, donor_id in enumerate(active_donors):
                params = {}
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                            WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                            CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_DONOR', probability: {1.0}}}]->(event)
                                            RETURN corr_1"""
                params["donor_id"] = donor_id
                params["current_event_id"] = current_event_id
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            # q2
            cypher_query = f"""MATCH (h:Entity {{type: 'hcw'}})-[:CORR]->(e:Event)
                                            WHERE id(e) = $current_event_id
                                            RETURN id(h) AS hcw_id"""
            params = {}
            params["current_event_id"] = current_event_id
            result = tx.run(cypher_query, **params)
            current_hcw = result.single()[0]
            if "active_hcws" in custom_attributes.keys():
                previous_hcws = custom_attributes["active_hcws"]
                new_hcws = list(set(previous_hcws + [current_hcw]))
                return_attributes["active_hcws"] = new_hcws
            else:
                return_attributes["active_hcws"] = [current_hcw]
            # q3
            if "hcw_id_mapping" in custom_attributes.keys():
                previous_mapping = custom_attributes["hcw_id_mapping"]
                previous_mapping[current_hcw] = event_attributes["hcw"]
                return_attributes["hcw_id_mapping"] = previous_mapping
            else:
                return_attributes["hcw_id_mapping"] = {current_hcw: event_attributes["hcw"]}

            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, False
            else:
                return False, return_attributes, False

    # DONE
    def rule_hand_hygiene(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 performs hand hygiene at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hand_hygiene does not have a corr key {key}"
            return corr_type, corr_prob

        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            return_attributes = {}
            # q1
            if "active_donors" in custom_attributes.keys():
                active_donors = custom_attributes["active_donors"]
            else:
                active_donors = []
            n = len(active_donors)
            params = {}
            keep_corrs = []
            for i, donor_id in enumerate(active_donors):
                cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                            WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                            CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBJECT', probability: '{str(1/n)+"?"}'}}]->(event)
                                            CREATE (donor)-[corr_2:CORR {{specialization: 'HAS_OBSERVER', probability: '{str(1-(1/n))+"?"}'}}]->(event)
                                            RETURN corr_1, corr_2"""
                params["donor_id"] = donor_id
                params["current_event_id"] = current_event_id
                result = tx.run(cypher_query, **params)
                new_corrs = [record.values() for record in result]
                for new_correlation in new_corrs[0]:
                    new_element_id = new_correlation.element_id.split(":")[-1]
                    new_element_start = new_correlation.start_node.element_id.split(":")[-1]
                    new_element_end = new_correlation.end_node.element_id.split(":")[-1]
                    new_element_type = new_correlation.type
                    new_element_properties = new_correlation._properties
                    keep_corrs.append([new_element_id,
                                                        new_element_start, new_element_end,
                                                        new_element_type, new_element_properties])
            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, False
            else:
                return False, return_attributes, False

    # DONE
    def rule_check_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 checks blood drawing machine ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_CHECK_TARGET"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_check_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_apply_tourniquet(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 applies tourniquet on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_apply_tourniquet does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_disinfect_injection_site(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 disinfects injection site on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_disinfect_injection_site does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_perform_venipuncture(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 performs venipuncture on D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_perform_venipuncture does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_remove_tourniquet(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 removes tourniquet from D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_remove_tourniquet does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_activate_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 activates the blood drawing machine MR at R
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_activate_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_monitor_patient(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 monitors D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_monitor_patient does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_stop_machine(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 stops blood drawing machine ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_stop_machine does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_remove_needle(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H2 removes needle from D1 at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_OBJECT"
                corr_prob = 1
            elif key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_remove_needle does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            this_donor = None
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            if not this_donor:
                raise "This donor does not exist (should be impossible)"
            for d in custom_attributes["active_donors"]:
                if d != this_donor:
                    params = {}
                    cypher_query = f"""MATCH (donor:Entity), (event:Event)
                                                WHERE id(donor) = $donor_id AND id(event) = $current_event_id
                                                CREATE (donor)-[corr_1:CORR {{specialization: 'HAS_OBSERVER', probability: {1}}}]->(event)
                                                RETURN corr_1"""
                    params["donor_id"] = d
                    params["current_event_id"] = current_event_id
                    tx.run(cypher_query, **params)
            # q2
            keep_corrs = []
            resolved_ambiguity = []
            current_hcw = event_attributes["hcw"] # H1
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            ambiguous_hcw = all_events[int(v[1])][3]["hcw"]
                            params = {}
                            if current_hcw == ambiguous_hcw:
                                if custom_attributes["donor_station_mapping"][int(v[0])] == event_attributes["station"]:
                                    if v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                else:
                                    if v[3]["specialization"]=="HAS_OBSERVER":
                                        params["new_probability"] = 1.0
                                        v[3]["probability"] = 1.0
                                    elif v[3]["specialization"]=="HAS_OBJECT":
                                        params["new_probability"] = 0.0
                                        v[3]["probability"] = 0.0
                                cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                                params["relationship_id"] = int(k)
                                tx.run(cypher_query, **params)
                                keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                                resolved_ambiguity.append([k, v, ambiguous_hcw])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            if len(keep_corrs) > 1:
                return keep_corrs, False, resolved_ambiguity
            else:
                return False, False, False

    # DONE
    def rule_donor_checkout(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # D1 checks out at L
        if mode == "corr":
            if key == "donor":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_donor_checkout does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            keep_corrs = []
            resolved_ambiguity = []
            for k, v in custom_attributes["donor_station_mapping"].items():
                if v == event_attributes["station"]:
                    this_donor = k
            for k, v in ambiguous_corr.items():
                if int(v[0]) in custom_attributes["active_donors"]:
                    if int(v[1]) in all_events.keys():
                        if all_events[int(v[1])][1] == "Hand hygiene":
                            params = {}
                            if int(v[0]) == this_donor:
                                params["new_probability"] = 0.0
                                v[3]["probability"] = 0.0
                            else:
                                if v[3]["specialization"]=="HAS_OBSERVER":
                                    params["new_probability"] = 0.0
                                    v[3]["probability"] = 0.0
                                elif v[3]["specialization"]=="HAS_OBJECT":
                                    params["new_probability"] = 1.0
                                    v[3]["probability"] = 1.0
                            cypher_query = f"""MATCH ()-[r]->()
                                                            WHERE id(r) = $relationship_id
                                                            SET r.probability = $new_probability
                                                            RETURN r"""
                            params["relationship_id"] = int(k)
                            tx.run(cypher_query, **params)
                            keep_corrs.append([k, v[0], v[1], v[2], v[3]])
                            resolved_ambiguity.append([k, v, None])
                        else:
                            raise "Currently only hh ambiguity implemented (event name wrong)"
                    else:
                        raise "Currently only hh ambiguity implemented (event wrong)"
                else:
                    raise "Currently only hh ambiguity implemented (donor wrong)"
            # q2
            return_attributes = {}
            if "donor_station_mapping" in custom_attributes.keys():
                if "active_donors" in custom_attributes.keys():
                    previous_donors = custom_attributes["active_donors"]
                    if this_donor in previous_donors:
                        previous_donors.remove(this_donor)
                    else:
                        raise "Safeguarding previous donors removal"
                    if len(previous_donors) > 0:
                        return_attributes["active_donors"] = previous_donors
                if "donor_station_mapping" in custom_attributes.keys():
                    previous_mapping = custom_attributes["donor_station_mapping"]
                    if this_donor in previous_mapping.keys():
                        del previous_mapping[this_donor]
                    else:
                        raise "Safeguarding previous donor-mapping removal"
                    if len(previous_mapping.keys()) > 0:
                        return_attributes["donor_station_mapping"] = previous_mapping

            if len(keep_corrs) > 1:
                return keep_corrs, return_attributes, resolved_ambiguity
            else:
                return False, return_attributes, False

    # DONE
    def rule_take_out_samples(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # H1 takes out the samples from ML at L
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "machine":
                corr_type = "HAS_MACHINE"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_take_out_samples does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            return False, False, False

    # DONE
    def rule_hcw_checkout(self, mode, key, custom_attributes=None, ambiguous_corr=None, all_events=None):
        # HCW H1 checks out at C
        if mode == "corr":
            if key == "hcw":
                corr_type = "HAS_SUBJECT"
                corr_prob = 1
            elif key == "station":
                corr_type = "HAS_LOCATION"
                corr_prob = 1
            else:
                raise f"rule_hcw_checkout does not have a corr key {key}"
            return corr_type, corr_prob
        # No integration query needed
        elif mode == "integration":
            tx, event_name, event_attributes, current_event_id = key
            # q1
            return_attributes = {}
            if "hcw_id_mapping" in custom_attributes.keys():
                for k, v in custom_attributes["hcw_id_mapping"].items():
                    if v == event_attributes["hcw"]:
                        this_hcw = k
                if "active_hcws" in custom_attributes.keys():
                    previous_hcws = custom_attributes["active_hcws"]
                    if this_hcw in previous_hcws:
                        previous_hcws.remove(this_hcw)
                    else:
                        raise "Safeguarding previous hcws removal"
                    if len(previous_hcws) > 0:
                        return_attributes["active_hcws"] = previous_hcws
                if "hcw_id_mapping" in custom_attributes.keys():
                    previous_mapping = custom_attributes["hcw_id_mapping"]
                    if this_hcw in previous_mapping.keys():
                        del previous_mapping[this_hcw]
                    else:
                        raise "Safeguarding previous hcw-mapping removal"
                    if len(previous_mapping.keys()) > 0:
                        return_attributes["hcw_id_mapping"] = previous_mapping

            return False, return_attributes, False
>>>>>>> Stashed changes
