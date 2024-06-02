# Event Knowledge Graphs with Ambiguity

This repository contains a prototype implementation of a framework for the online construction of an Event Knowledge Graph with Ambiguity from a stream of ambiguous events.

The prototype implementation accompanies the paper "Event Knowledge Graphs with Ambiguity" by Marco Franceschetti, Dominik Buchegger, Ronny Seiger, and Barbara Weber, submitted to ICPM 2024.

The implementation requires Neo4J as the graph database to store the graph. To run it, execute python main.py.

The "results" folder contains the csv files and graphs with the measured times (in seconds) for processing each new event in a sample stream created for a simulated blood donation process involving two donors and two healthcare workers. The measurements have been repeated 20 times.
