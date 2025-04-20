package org.apache.zeppelin.iginx.interpreter.dataproperty.network;

import java.util.*;

public class NetworkTreeNode {
  private String id;
  private String name;
  private Map<String, NetworkTreeNode> children = new HashMap<>();
  private int depth;
  private String mergedRoot;
  private boolean isExpanded;
  private boolean isShown;
  private boolean isMergedNode;

  public NetworkTreeNode(String id, String name, int depth) {
    this(id, name, depth, false);
  }

  public NetworkTreeNode(String id, String name, int depth, boolean isMergedNode) {
    this.id = id;
    this.name = name.replace("[^a-zA-Z0-9]", " ");
    this.depth = depth;
    this.isExpanded = false;
    this.isShown = false;
    this.isMergedNode = isMergedNode;
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

  public Map<String, NetworkTreeNode> getChildren() {
    return children;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public void setMergedRoot(String mergedRoot) {
    this.mergedRoot = mergedRoot;
    this.setDepth(depth + mergedRoot.split("\\.").length);
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

  public boolean isMergedNode() {
    return isMergedNode;
  }

  public String getMergedRoot() {
    return mergedRoot;
  }

  public void setMergedNode(boolean mergedNode) {
    isMergedNode = mergedNode;
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
