from abc import abstractmethod
import numpy as np


class UDFBaseInsert:
    """
    Base class for handling insert operations into a data store.
    Subclasses (e.g., UDFDefaultInsert) should implement domain-specific
    logic to connect and store the data.
    """

    @abstractmethod
    def insert(self, paths: list[str], types: list[str], descriptions: list[str], embeddings: list[np.ndarray]) -> \
    tuple[int, int]:
        """
        Handle the actual insertion of data in subclasses.

        :param paths: The list of item paths.
        :param types: The list of item types, possibly None.
        :param descriptions: The list of text descriptions for each item.
        :param embeddings: The list of numeric embeddings (e.g., vectors).
        :return: A tuple containing the number of successfully inserted records and failures.
        """
        pass

    def transform(self, data, args, kvargs):
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        path_index = data[0].index('path')
        type_index = data[0].index('type')
        description_index = data[0].index('description')
        embedding_index = data[0].index('embedding')

        paths = [row[path_index].decode('utf-8') for row in data[2:]]
        types = [row[type_index].decode('utf-8') if row[type_index] else None for row in data[2:]]
        descriptions = [row[description_index].decode('utf-8') for row in data[2:]]
        embeddings = [np.frombuffer(row[embedding_index], dtype=np.float32) for row in data[2:]]

        inserted, failed = self.insert(paths, types, descriptions, embeddings)

        return [
            ["(inserted)", "(failed)"],
            ["LONG", "LONG"],
            [inserted, failed],
        ]
