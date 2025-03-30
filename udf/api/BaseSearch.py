from abc import abstractmethod

import numpy as np

class UDFBaseSearch:
    @abstractmethod
    def search(self, pattern: str, embedding: np.ndarray, limit: int) -> list[tuple[str, str, str, float]]:
        pass

    @abstractmethod
    def describe_similarity(self, source_description: str, target_descriptions: list[str]) ->  list[str]:
        pass

    def transform(self, data, args, kvargs):
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        pattern = kvargs["pattern"].decode("utf-8")
        limit = int(kvargs["topk"])
        assert len(data) == 3

        description_index = data[0].index('description')
        embedding_index = data[0].index('embedding')

        description = data[2][description_index].decode("utf-8")
        embedding = np.frombuffer(data[2][embedding_index], dtype=np.float32)

        result = self.search(pattern, embedding, limit)

        paths = [row[0] for row in result]
        types = [row[1] for row in result]
        target_descriptions = [row[2] for row in result]
        scores = [row[3] for row in result]

        similarities = self.describe_similarity(description, target_descriptions)

        return [
            ['(path)','(type)','(score)','(similarity)'],
            ['BINARY','BINARY','DOUBLE','BINARY'],
        ] + [
            [path.encode(), type.encode() if type else None,score, similarity.encode()]
            for path, type,score, similarity in zip(paths, types, scores, similarities)
        ]
