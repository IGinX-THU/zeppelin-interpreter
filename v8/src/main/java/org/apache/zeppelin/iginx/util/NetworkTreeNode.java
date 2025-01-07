package org.apache.zeppelin.iginx.util;

import java.util.*;

public class NetworkTreeNode {
  private String id;
  private String name;
  private String parent;
  private Map<String, NetworkTreeNode> children = new HashMap<>();
  private int depth;
  private List<Double> embedding;

  public NetworkTreeNode(String id, String name, String parent, int depth) {
    this.id = id;
    this.name = name;
    this.parent = parent;
    this.depth = depth;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getParent() {
    return parent;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }

  public Map<String, NetworkTreeNode> getChildren() {
    return children;
  }

  public void setChildren(Map<String, NetworkTreeNode> children) {
    this.children = children;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public List<Double> getEmbedding() {
    return embedding;
  }

  public void setEmbedding(List<Double> embedding) {
    this.embedding = embedding;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String indentation = getIndentation(depth);

    sb.append(indentation)
        .append("NetworkTreeNode{")
        .append("id='")
        .append(id)
        .append('\'')
        .append(", name='")
        .append(name)
        .append('\'')
        .append(", parent='")
        .append(parent)
        .append('\'')
        .append(", depth=")
        .append(depth);

    // 打印子节点，递归调用toString方法，控制缩进
    if (!children.isEmpty()) {
      sb.append(", children=[\n");
      for (NetworkTreeNode child : children.values()) {
        sb.append(child.toString()).append(",\n");
      }
      // 去除最后一个多余的逗号和换行符
      sb.setLength(sb.length() - 2);
      sb.append("\n").append(indentation).append("]");
    }

    sb.append("}");
    return sb.toString();
  }

  private String getIndentation(int depth) {
    StringBuilder indentation = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      indentation.append("  "); // 每层增加两个空格
    }
    return indentation.toString();
  }
}
