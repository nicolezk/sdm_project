from neo4j import GraphDatabase
import pandas as pd

import time

uri = "neo4j:////localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "sdm123"))

with driver.session() as session: 
    # Catalog of commands
    node_commands = {
        'species': 
         "LOAD CSV WITH HEADERS FROM 'file:///food_web_vertices.csv' AS line " +
         "FIELDTERMINATOR ';'" +
         "MERGE (:species {id: toInteger(line.id), name: line.name})"
     }
        
    index_commands = {
        'species':
            "CREATE INDEX ix_species FOR (n:species) ON n.id"
    }

    edge_commands = {
        'conference_compose_edition':
            "LOAD CSV WITH HEADERS FROM 'file:///food_web_edges.csv' AS line " +
            "FIELDTERMINATOR ';'" +
            "MATCH (src:species {id:toInteger(line.src)}), (dst:species {id:toInteger(line.dst)}) " +
            "MERGE (src)-[:feeds_on]->(dst)"
    }

    projection_commands = {
        'species':
            "CALL gds.graph.create('species', 'species', 'feeds_on')"
    }

    algorithm_commands = {
        'page_rank':
            "CALL gds.pageRank.stream('species', {dampingFactor:0.85, tolerance:0.0001}) " +
            "YIELD nodeId, score " +
            "RETURN gds.util.asNode(nodeId).name AS species, score " +
            "ORDER BY score DESC, species ASC"
        ,
        'betweenness':
            "CALL gds.betweenness.stream('species') " +
            "YIELD nodeId, score " +
            "RETURN gds.util.asNode(nodeId).name AS species, score " +
            "ORDER BY score DESC, species ASC"
    } 

    # Execute commands corresponding to the command catalog
    for node in node_commands.keys():
        start_time = time.time()
        session.run(node_commands[node])
        print("Creating the nodes ran in --- %s seconds ---" % (time.time() - start_time))
        
    for index in index_commands.keys():
        start_time = time.time()
        session.run(index_commands[index])
        print("Creating the index ran in --- %s seconds ---" % (time.time() - start_time))
        
    for edge in edge_commands.keys():
        start_time = time.time()
        session.run(edge_commands[edge])
        print("Creating the edges ran in --- %s seconds ---" % (time.time() - start_time))

    for projection in projection_commands.keys():
        start_time = time.time()
        session.run(projection_commands[projection])
        print("Creating the projection ran in --- %s seconds ---" % (time.time() - start_time))

    results = {}
    for algorithm in algorithm_commands.keys():
        start_time = time.time()
        pd.DataFrame(session.run(algorithm_commands[algorithm]), columns=['species', algorithm]).to_csv(algorithm+'_results_neo4j.csv')
        print(algorithm + " ran in --- %s seconds ---" % (time.time() - start_time))
    
driver.close()
