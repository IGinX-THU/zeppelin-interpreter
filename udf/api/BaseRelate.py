import json
from abc import abstractmethod


class UDFBaseRelate:
    @abstractmethod
    def relate(self, source_paths: list[str], target_paths: list[str]) -> list[tuple[str, str, float, str]]:
        pass

    def transform(self, data, args, kvargs: dict):
        source_paths = json.loads(kvargs["sources"].decode("utf-8"))
        target_paths = json.loads(kvargs["targets"].decode("utf-8"))
        relations = self.relate(source_paths, target_paths)
        return [
            ["(source)", "(target)", "(score)", "(description)"],
            ['BINARY', 'BINARY', 'DOUBLE', 'BINARY'],
        ] + [
            [source.encode(), target.encode(), score, similarity.encode()]
            for source, target, score, similarity in relations
        ]
