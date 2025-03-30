package cn.edu.tsinghua;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class SchemaPileDatasetTest {

  class Node {
    Map<String, Node> children;

    public Node() {
      this.children = new HashMap<>();
    }

    public void insert(String[] parts) {
      if (parts.length == 0) {
        return;
      }
      String node = parts[0];
      String[] rest = Arrays.copyOfRange(parts, 1, parts.length);
      children.computeIfAbsent(node, k -> new Node()).insert(rest);
    }
  }

  @Test
  void testShowLevelNumber() throws IOException {
    SchemaPileDataset schemaPileDataset = new SchemaPileDataset();
    List<String> paths = schemaPileDataset.data.stream().map(IginxColumn::getPath).collect(Collectors.toList());

    Node root = new Node();

    paths.stream().map(path -> path.split("\\.")).
        map(parts -> new String[]{
            String.join(".", Arrays.copyOf(parts, parts.length - 2)),
            parts[parts.length - 2],
            parts[parts.length - 1],
        }).forEach(root::insert);
// 统计 root 各层节点数量
    Map<Integer, Integer> levelCount = new HashMap<>();

    countLevels(root, 0, levelCount);

    levelCount.forEach((level, count) -> System.out.println("Level " + level + ": " + count + " nodes"));
  }

  private void countLevels(Node node, int level, Map<Integer, Integer> levelCount) {
    levelCount.put(level, levelCount.getOrDefault(level, 0) + 1);

    for (Node child : node.children.values()) {
      countLevels(child, level + 1, levelCount);
    }


  }
}