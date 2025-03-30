import shelve

import numpy as np
from sentence_transformers import SentenceTransformer
import tqdm

from api.BaseDescribe import UDFBaseDescribe

class UDFDefaultDescribe(UDFBaseDescribe):

    def _build_descriptions(self, paths: list[str]) -> dict[str, str]:
        print("building descriptions for", len(paths), "paths")
        nodes_list = [path.split(".") for path in paths]
        tree = self._build_path_tree(nodes_list)

        descriptions = {}
        self._dfs_build_descriptions(tree, [], descriptions)
        return descriptions

    def _build_path_tree(self, nodes_list: list[list[str]]) -> dict:
        print("building path tree for", len(nodes_list), "paths")

        tree = {}
        for nodes in nodes_list:
            current_node = tree
            for node in nodes:
                current_node = current_node.setdefault(node, {})
        return tree

    def _dfs_build_descriptions(self, tree, path_nodes: list[str], descriptions: dict[str, str]) -> None:
        for key, value in tree.items():
            path_nodes.append(key)
            description_nodes = list(reversed(path_nodes))
            if not value:
                description_nodes += list(tree.keys())
            else:
                description_nodes += list(value.keys())
            descriptions[".".join(path_nodes)] = ", ".join(description_nodes)
            self._dfs_build_descriptions(value, path_nodes, descriptions)
            path_nodes.pop()

    def describe(self, leaf_paths_with_type: dict[str, str]) -> dict[str, str]:
        leaf_paths = list(leaf_paths_with_type.keys())
        return self._build_descriptions(leaf_paths)
