package org.apache.zeppelin.iginx.util;

import java.util.ArrayList;
import java.util.List;

public class TreeNode {
  String path;
  String value;
  List<TreeNode> children;
  List<Double> embedding;

  public TreeNode(String path, String value, List<Double> embedding) {
    this.path = path;
    this.value = value;
    this.children = new ArrayList<>();
    this.embedding = embedding;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public List<TreeNode> getChildren() {
    return children;
  }

  public void setChildren(List<TreeNode> children) {
    this.children = children;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public List<Double> getEmbedding() {
    return embedding;
  }

  public void setEmbedding(List<Double> embedding) {
    this.embedding = embedding;
  }
}
