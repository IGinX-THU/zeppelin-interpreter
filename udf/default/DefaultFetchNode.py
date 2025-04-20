from neo4j import GraphDatabase
from typing import List, Tuple

from api.BaseFetchNode import UDFBaseFetchNode
from default.DefaultUtilities import NEO4J_HOST, NEO4J_PORT, NEO4J_USERNAME, NEO4J_PASSWORD


class UDFDefaultFetchNode(UDFBaseFetchNode):

    def __init__(self, neo4j_host=NEO4J_HOST, neo4j_port=NEO4J_PORT, neo4j_username=NEO4J_USERNAME, neo4j_password=NEO4J_PASSWORD):
        self.neo4j_url = f"bolt://{neo4j_host}:{neo4j_port}"
        self.driver = GraphDatabase.driver(self.neo4j_url, auth=(neo4j_username, neo4j_password))

    def close(self):
        self.driver.close()

    def fetch(self, parent_path: str) -> List[Tuple[str, str, int]]:
        print(f"fetching parent_path: {parent_path}")
        query = (
            "MATCH (parent:Node)-[:CONTAIN]->(child:Node) "
            "WHERE parent.path = $parentPath "
            "RETURN child.path AS path, child.name AS name, child.level AS level"
        )

        with self.driver.session() as session:
            result = session.execute_read(
                lambda tx: [
                    (record["path"], record["name"], record["level"])
                    for record in tx.run(query, parentPath=parent_path)
                ]
            )

            return result


if __name__ == "__main__":
    fetcher = UDFDefaultFetchNode()
    test_parent_path = "rootId"
    try:
        children = fetcher.fetch(test_parent_path)
        print(f"Children of {test_parent_path}: {children}")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        fetcher.close()
