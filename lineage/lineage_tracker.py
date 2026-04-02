from lineage.neo4j_client import driver

def store_lineage(source, transformation, target):
    query = """
    MERGE (s:Dataset {name: $source})
    MERGE (t:Transformation {name: $transformation})
    MERGE (d:Dataset {name: $target})
    MERGE (s)-[:INPUT_TO]->(t)
    MERGE (t)-[:OUTPUT_TO]->(d)
    """

    with driver.session() as session:
        session.run(
            query,
            source=source,
            transformation=transformation,
            target=target
        )