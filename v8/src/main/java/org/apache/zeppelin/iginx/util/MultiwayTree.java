package org.apache.zeppelin.iginx.util;

import java.lang.reflect.Constructor;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.iginx.util.algorithm.mergeforest.MergeForestStrategy;

public class MultiwayTree {
  public static final String ROOT_NODE_NAME = "数据资产";
  public static final String ROOT_NODE_PATH = "root";

  public TreeNode getRoot() {
    return root;
  }

  public void setRoot(TreeNode root) {
    this.root = root;
  }

  TreeNode root;

  public TreeNode insert(TreeNode parenNode, TreeNode newNode) {
    TreeNode childNode = findNode(parenNode, newNode);
    if (childNode != null) {
      System.out.println("node already exists");
    } else {
      parenNode.children.add(newNode);
      return newNode;
    }
    return childNode;
  }

  // 查找节点操作
  private TreeNode findNode(TreeNode node, TreeNode nodeToFind) {
    if (node == null) {
      return null;
    }
    for (TreeNode child : node.children) {
      if (child.value.equals(nodeToFind.value)) {
        return child;
      }
    }
    return null;
  }

  public void traversePreorder(TreeNode node) {
    if (node != null) {
      System.out.print(node.value + " ");
      for (TreeNode child : node.children) {
        traversePreorder(child);
      }
    }
  }

  public static MultiwayTree getMultiwayTree() {
    MultiwayTree tree = new MultiwayTree();
    tree.root = new TreeNode(ROOT_NODE_PATH, ROOT_NODE_NAME, null); // 初始化
    return tree;
  }

  public static void addTreeNodeFromString(MultiwayTree tree, String nodeString) {
    String[] nodes = nodeString.split("\\.");
    TreeNode newNode = tree.root;
    for (int i = 0; i < nodes.length; i++) {
      List<Double> embedding = EmbeddingUtils.getEmbedding(nodes[i]);
      newNode =
          tree.insert(
              newNode,
              new TreeNode(StringUtils.join(newNode.path, ".", nodes[i]), nodes[i], embedding));
    }
  }

  public static void mergeTree(MultiwayTree tree, String strategy) throws Exception {
    Class<?> mergeClass =
        Class.forName("org.apache.zeppelin.iginx.util.algorithm.mergeforest." + strategy);
    Constructor<?> constructor = mergeClass.getConstructor();
    MergeForestStrategy mergeForestStrategy = (MergeForestStrategy) constructor.newInstance();
    mergeForestStrategy.mergeForest(tree.root);
  }
}
