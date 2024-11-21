# Event Knowledge Graphs ...

This repository contains a prototype implementation of a framework for the online construction of an Event Knowledge Graph from a stream of events.

The prototype implementation accompanies the paper "..." by Marco Franceschetti, Dominik Buchegger, Ronny Seiger, and Barbara Weber, submitted to ....

The implementation requires Neo4J as the graph database to store the graph. To run it, edit lines 19-27 in the *main.py* file to set the Neo4j connection setting and the streaming input. Then execute ```python main.py```.

The "results" folder contains the csv files and graphs with the measured times (in seconds) for processing each new event in a sample stream.
