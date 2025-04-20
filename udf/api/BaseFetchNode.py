from abc import abstractmethod


class UDFBaseFetchNode:
    """
    Base class for fetching records based on a parent node path from neo4j.
    Derived classes should override the fetch() method to query and
    retrieve records, including path, name and level.
    """

    @abstractmethod
    def fetch(self, parent_path: str) -> list[tuple[str, str, int]]:
        """
        Retrieves records matching a given parent path.

        Args:
            parent_path (str): The path of the parent node to fetch child nodes from.

        Returns:
            list[tuple[str, str, int]]: Fetched records.
        """
        pass

    def transform(self, data, args, kvargs):
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        parent_path = kvargs["path"].decode("utf-8")
        result = self.fetch(parent_path)

        return [
            ['(path)', '(name)', '(level)'],
            ['BINARY', 'BINARY', 'LONG'],
        ] + [
            [path.encode(), name.encode(), level]
            for path, name, level in result
        ]
