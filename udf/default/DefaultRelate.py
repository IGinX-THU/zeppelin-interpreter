import pandas as pd

from pymilvus import connections, Collection

from api.BaseRelate import UDFBaseRelate
from default.DefaultUtilities import MILVUS_HOST, MILVUS_PORT, MILVUS_COLLECTION, LLMDao


class UDFDefaultRelate(UDFBaseRelate):
    def __init__(self, milvus_host=MILVUS_HOST, milvus_port=MILVUS_PORT, collection_name = MILVUS_COLLECTION):
        self.milvus_host = milvus_host
        self.milvus_port = milvus_port
        self.collection_name = collection_name

    def relate(self, source_paths: list[str], target_paths: list[str]) -> list[tuple[str, str, float, str]]:
        print("fetching by path")

        connections.connect(host=self.milvus_host, port=self.milvus_port)
        collection = Collection(self.collection_name)

        entities = collection.query(
            output_fields=["path", "embedding", "description"],
            expr=f"path in {source_paths}"
        )

        source_paths = [entity["path"] for entity in entities]
        source_embeddings = [entity["embedding"] for entity in entities]
        source_descriptions = [entity["description"] for entity in entities]

        print("Searching similar embeddings")
        hits_each = collection.search(
            expr = f"path in {target_paths}",
            data=source_embeddings,
            anns_field="embedding",
            output_fields=["path", "description"],
            limit=10,
            param={"metric_type": "COSINE"},
        )

        relations = [
            (source_path, hit.fields["path"], hit.distance, source_description, hit.fields["description"])
            for i, (source_path, source_description) in enumerate(zip(source_paths, source_descriptions))
            for hit in hits_each[i]
        ]

        if not relations:
            return []

        # Create a DataFrame from the relations list with columns: source, target, and similarity
        df = pd.DataFrame(relations,
                          columns=["source", "target", "similarity", "source_description", "target_description"])

        # Extract the root part (before the first dot) from source and target columns
        df['source_root'] = df['source'].str.split('.').str[0]
        df['target_root'] = df['target'].str.split('.').str[0]

        # Filter out records where source_root and target_root are the same
        df = df[df['source_root'] != df['target_root']]

        # Find the maximum similarity for each unique pair of source_root and target_root
        max_similarities = df.loc[
            df.groupby([df[['source_root', 'target_root']].apply(frozenset, axis=1)])['similarity'].idxmax()]

        # Select only the source, target, and similarity columns from the DataFrame
        relations = max_similarities[['source', 'target', 'similarity', 'source_description', 'target_description']]

        # Select top 1/3 of the most relations based on similarity
        limit = 3
        relations = relations.sort_values(by='similarity', ascending=False).head(limit)

        if len(relations) == 0:
            return []

        prompts = [
            f"""
            You are a concept master. 
            Please provide a concise and specific description of the relationship 
            between "{source_description}" and "{target_description}" in one sentence using no more than 10 words and without any punctuation. 
            Only the summary result is needed. Ensure the summary does not exceed 10 words and is concise and logical phrase.
            """
            for source_description, target_description in zip(relations['source_description'], relations['target_description'])
        ]
        # 并行获取 source_description 和 target_description 的关系的描述
        relations['description'] = LLMDao().request(prompts)
        relations = relations[['source', 'target', 'similarity', 'description']]

        return [
            (source, target, score, description)
            for source, target, score, description in relations.values
        ]


if __name__ == '__main__':
    # 本地测试
    udf = UDFDefaultRelate()
    kvargs = {'sources': b'["1612SMShuvo_sub_contract","2010USFJava_MaurerP","1lirisist_kanbank"]', 'port': b'19530', 'targets': b'["1lirisist_kanbank","1ibrary_1ibrary_back_end"]'}
    result = udf.transform([[],[]], [], kvargs)
    print(result)
