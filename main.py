from datetime import datetime
from neo4j import GraphDatabase
import importlib
import time
import matplotlib.pyplot as plt
#  Include the packages despite not in the same package / not installed via pip.
module_name = "EKG_base"
class_name = "BaseEKG"
module = importlib.import_module(module_name)
imported_base = getattr(module, class_name)
#  Include the packages despite not in the same package / not installed via pip.
module_name = "EKG_simulation"
class_name = "SimulationEKG"
module = importlib.import_module(module_name)
imported_simulation = getattr(module, class_name)


#######################################################################################
#
# User defined
#
#######################################################################################
# Neo4j connection settings
uri = "neo4j://localhost:7687"
user = "neo4j"
password = "12345678"
# Define rules
rules = True # True / False
module_name = "EKG_rules" # / just set rules to False
class_name = "RulesEKG" #  / just set rules to False



#######################################################################################
#
# Main
#
#######################################################################################
# ------------> 1: G ← new EKGA <---------------- #
if rules:
    module = importlib.import_module(module_name)
    imported_rules = getattr(module, class_name)
    rules = imported_rules()
simulator = imported_base(uri, user, password, rules)
# ------------> 2: for all event e ∈ s do <---------------- #
durations = []
event_indices = []
for index, event in enumerate(imported_simulation()):
    input("Press Enter for Event")
    start_time = time.time()
    simulator.process_event(event)
    end_time = time.time()
    durations.append(end_time - start_time)
    event_indices.append(index + 1)
simulator.close()
# ------------> 20: end for <---------------- #

# Plot the durations
plt.figure(figsize=(10, 6))
plt.plot(event_indices, durations, marker='o')
plt.xlabel('Event Index')
plt.ylabel('Processing Time (seconds)')
plt.title('Processing Time for Each Event')
plt.grid(True)
plt.show()
