package org.apache.zeppelin.iginx.util;

import java.util.*;

public class NetworkTreeNode {
  private String id;
  private String name;
  private Map<String, NetworkTreeNode> children = new HashMap<>();
  private int depth;
  private String mergedRoot;
  private List<Float> embedding;
  private Boolean isExpanded;
  private Boolean isShown;

  public NetworkTreeNode(String id, String name, int depth) {
    this.id = id;
    this.name = name;
    this.depth = depth;
    this.isExpanded = false;
    this.isShown = false;
  }

  public String getNetworkId() {
    if (mergedRoot == null) return id;
    else {
      String[] parts = id.split("\\.", 2);
      return parts[0] + "." + mergedRoot + "." + parts[1];
    }
  }

  public String getEmbeddingId() {
    String[] parts = id.split("\\.", 2);
    return parts.length > 1 ? parts[1] : "";
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

  public Map<String, NetworkTreeNode> getChildren() {
    return children;
  }

  public void setChildren(Map<String, NetworkTreeNode> children) {
    this.children = children;
  }

  public int getDepth() {
    if (mergedRoot == null) {
      return depth;
    } else {
      return depth + 1;
    }
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public String getMergedRoot() {
    return mergedRoot;
  }

  public void setMergedRoot(String mergedRoot) {
    this.mergedRoot = mergedRoot;
  }

  public List<Float> getEmbedding() {
    return embedding;
  }

  public void setEmbedding(List<Float> embedding) {
    this.embedding = embedding;
  }

  public Boolean getExpanded() {
    return isExpanded;
  }

  public void setExpanded(Boolean expanded) {
    isExpanded = expanded;
  }

  public Boolean getShown() {
    return isShown;
  }

  public void setShown(Boolean shown) {
    isShown = shown;
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
