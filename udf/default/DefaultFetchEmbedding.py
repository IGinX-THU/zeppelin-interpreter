import numpy as np

from api.BaseFetchEmbedding import UDFBaseFetchEmbedding
from default.DefaultUtilities import MILVUS_HOST, MILVUS_PORT, MILVUS_COLLECTION
from pymilvus import connections, Collection


class UDFDefaultFetchEmbedding(UDFBaseFetchEmbedding):

    def __init__(self, milvus_host=MILVUS_HOST, milvus_port=MILVUS_PORT, collection_name=MILVUS_COLLECTION):
        self.milvus_host = milvus_host
        self.milvus_port = milvus_port
        self.collection_name = collection_name

    def fetch(self, pattern: str, level: int) -> list[tuple[str, str, str, np.ndarray]]:
        print(f"fetching pattern: {pattern}, level: {level}")
        connections.connect(host=self.milvus_host, port=self.milvus_port)
        collection = Collection(self.collection_name)
        milvus_pattern = pattern.replace('*', '%')
        entities = collection.query(
            output_fields=["path", "embedding", "description"],
            expr=f"level == {level} and path like '{milvus_pattern}'"
        )
        return [
            (
                entity["path"],
                entity["type"] if "type" in entity else None,
                entity["description"],
                np.array(entity["embedding"], dtype=np.float32),
            )
            for entity in entities
        ]
