# Event Knowledge Graphs ...

This repository contains a prototype implementation of a framework for the online construction of an Event Knowledge Graph from a stream of events.

The prototype implementation accompanies the paper "..." by Marco Franceschetti, Dominik Buchegger, Ronny Seiger, and Barbara Weber, submitted to ....

The implementation requires Neo4J as the graph database to store the graph. To run it, execute python main.py.

**Add something about the parameters to run main.py. Need to implement xes_file_path & USE_MQTT = False (lines 25 & 26) parametrized to the starting instruction.**

The "results" folder contains the csv files and graphs with the measured times (in seconds) for processing each new event in a sample stream.
