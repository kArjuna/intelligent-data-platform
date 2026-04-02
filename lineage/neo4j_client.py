from neo4j import GraphDatabase

URI = "bolt://neo4j:7687"   # ✅ IMPORTANT FIX
USERNAME = "neo4j"
PASSWORD = "password"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))