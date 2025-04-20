from abc import abstractmethod


class UDFBaseDescribe:
    """
    UDFBaseDescribe is an abstract base class that defines the interface for describing leaf paths with their types.
    """

    @abstractmethod
    def describe(self, leaf_paths_with_type: dict[str, str]) -> dict[str, str]:
        """
        Abstract method to describe tree paths with their types.

        Args:
            leaf_paths_with_type (dict[str, str]): A dictionary where keys are leaf node paths and values are their types.

        Returns:
            dict[str, str]: A dictionary where keys are paths (include inner node path and leaf node path) and values are their descriptions.
        """
        pass

    def transform(self, data, args, kvargs):
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        pathIndex = data[0].index('path')
        typeIndex = data[0].index('type')
        leaf_paths_with_type = {row[pathIndex].decode('utf-8'): row[typeIndex].decode('utf-8') for row in data[2:]}
        paths_with_description = self.describe(leaf_paths_with_type)

        return [
            ['(path)', '(type)', '(description)'],
            ['BINARY', 'BINARY', 'BINARY'],
        ] + [
            [path.encode(), leaf_paths_with_type[path].encode() if path in leaf_paths_with_type else None,
             description.encode()]
            for path, description in paths_with_description.items()
        ]
