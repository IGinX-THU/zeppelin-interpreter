from abc import abstractmethod

import numpy as np


class UDFBaseEncode:
    """
    Base class for encoding text descriptions into numeric embeddings.
    Derived classes are expected to implement a specialized `encode` method
    that leverages a specific embedding model.
    """

    @abstractmethod
    def encode(self, descriptions: list[str]) -> list[np.ndarray]:
        """
        Encodes a list of text descriptions into numeric embeddings.

        Args:
            descriptions (list[str]): A list of text strings to encode.

        Returns:
            list[np.ndarray]: A list of numeric embedding arrays. Each array
            represents the embedding of the corresponding string in `descriptions`.
        """
        pass

    def transform(self, data, args, kvargs):
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        description_index = data[0].index('description')
        descriptions = [row[description_index].decode('utf-8') for row in data[2:]]
        embeddings = self.encode(descriptions)
        embeddings_bytes = [embedding.astype(np.float32).tobytes() for embedding in embeddings]

        return [
            ['(' + name + ')' for name in data[0]] + ['(embedding)'],
            data[1] + ['BINARY'],
        ] + [
            row + [embedding_bytes]
            for row, embedding_bytes in zip(data[2:], embeddings_bytes)
        ]
