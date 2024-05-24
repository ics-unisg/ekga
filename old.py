    def rule_hcw_checkin(self, mode, key):
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
            step, run_once, run_each, current_event_id, attr_key, attr_val = key

            if step == "once": # need to return (cypher_query, params) or False
                cypher_query = f"""MATCH (d:Entity {{type: 'donor'}})-[:CORR]->(e:Event {{name: 'Donor check-in'}})
                WHERE NOT (d)-[:CORR]->(:Event {{name: 'Donor check-out'}})
                RETURN id(d) AS donor_id"""
                params = {}
                return (cypher_query, params)


            elif step == "each": # need to return (cypher_query, params) or False
                if attr_key != "donor":
                    active_donors = [x[0] for x in run_once]
                    print("ACTIVEEEE", active_donors)
                    n = len(active_donors)
                    cypher_query = """"""
                    params = {}
                    keep_corrs = []
                    for i, donor_id in enumerate(active_donors):
                        donor_alias = f"d_{run_each}_{i}"
                        donor_id_alias = f"d_{run_each}_id_{i}"
                        event_alias = f"e_{run_each}_{i}"
                        current_event_alias = f"e_{run_each}_id_{i}"
                        probability_1_alias = f"probability_1_{run_each}_{i}"
                        probability_2_alias = f"probability_2_{run_each}_{i}"
                        corr_1_alias = f"corr_1_{run_each}_{i}"
                        corr_2_alias = f"corr_2_{run_each}_{i}"
                        keep_corrs.append(corr_1_alias)
                        keep_corrs.append(corr_2_alias)
                        cypher_query += f"""MATCH ({donor_alias}:Entity {{id: ${donor_id_alias}}}), ({event_alias}:Event {{id: ${current_event_alias}}})
                            CREATE ({donor_alias})-[{corr_1_alias }:CORR {{specialization: 'object', probability: ${probability_1_alias}}}]->({event_alias})
                            CREATE ({donor_alias})-[{corr_2_alias }:CORR {{specialization: 'observer', probability: ${probability_2_alias}}}]->({event_alias})
                        """
                        params[donor_id_alias] = donor_id
                        params[current_event_alias] = current_event_id
                        params[probability_1_alias] = 1/n
                        params[probability_2_alias] = 1-(1/n)
                    cypher_query += """RETURN """
                    cypher_query += ", ".join([""] + [x for x in keep_corrs])[2:]
                    return (cypher_query, params)
                else:
                    return False

            elif step == "final": # need to return ambiguity or False
                ambiguity = False
                if run_each:
                    ambiguity = [record.values() for record in run_each]
                if len(ambiguity) == 0:
                    ambiguity = False
                return ambiguity

            else:
                raise f"rule_hcw_checkin does not have a integration step {step}"

        else:
            return False










    def _create_prob_corrs(tx, event_name, event_attributes, event_specific_rule, current_event_id):
        # ------------> 11: Q ← GET INTEGRATION QUERIES(R) <---------------- #
        if event_specific_rule:
            get_specific_rule = event_specific_rule.get_rule(event_name)
            if not get_specific_rule:
                raise f"Missing rule for {event_name}"
            # ------------> 12: for all q ∈ Q do <---------------- #
            # ------------> 13: EXECUTE QUERY(G, q) <---------------- #
            result = get_specific_rule("integration", ["once", None, None, current_event_id, event_attributes, None])
            print("Used integration rule ONCE")
            run_once = None
            if result:
                print("TRYING", result)
                cypher_query, params = result
                result = tx.run(cypher_query, **params)
                run_once = [record.values() for record in result]
            print(f"Passing {run_once} from ONCE")

            all_results = []
            run_each = None
            for i, key in enumerate(event_attributes):
                result = get_specific_rule("integration", ["each", run_once, i, current_event_id, key, event_attributes[key]])
                print(f"Used integration rule EACH for {key}")
                if result:
                    cypher_query, params = result
                    print(cypher_query)
                    print(params)
                    result = tx.run(cypher_query, **params)
                    all_results.append(result)
                # ------------> 14: end for <---------------- #
            if len(all_results) != 0:
                print("WE HAVE", all_results)
                all_values = []
                for result in all_results:
                    print(result)
                    values = [record.values() for record in result]
                    print(values)
                    all_values.append(values)
                run_each = all_values
            print(f"Passing {run_each} from EACH")

            ambiguity = get_specific_rule("integration", ["final", run_once, run_each, current_event_id, event_attributes, None])
            print(f"Used integration rule FINAL with result: {ambiguity}")
            return ambiguity
