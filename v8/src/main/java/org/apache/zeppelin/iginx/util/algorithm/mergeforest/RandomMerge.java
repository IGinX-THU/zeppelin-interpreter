package org.apache.zeppelin.iginx.util.algorithm.mergeforest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.zeppelin.iginx.util.EmbeddingUtils;
import org.apache.zeppelin.iginx.util.LLMUtils;
import org.apache.zeppelin.iginx.util.TreeNode;

public class RandomMerge implements MergeForestStrategy {
  private static final Double SIMILARITY_THRESHOLD = 0.7;
  private static final Integer MAX_CONTINUOUS_FAILURE_COUNT = 5;

  @Override
  public void mergeForest(TreeNode root) {
    List<TreeNode> forest = root.getChildren();
    int targetTreeCount = (int) Math.ceil(Math.sqrt(forest.size()));
    Random random = new Random();
    int failureCount = 0; // 记录连续合并失败次数

    while (forest.size() > targetTreeCount && failureCount < MAX_CONTINUOUS_FAILURE_COUNT) {
      System.out.println("当前森林的大小为: " + forest.size());

      // 随机选择一个根节点
      TreeNode referenceNode = forest.get(random.nextInt(forest.size()));
      List<TreeNode> selectedNodes = new ArrayList<>();
      selectedNodes.add(referenceNode);

      // 计算其他根节点与 referenceNode 的相似性并排序
      List<TreeNode> otherNodes = new ArrayList<>(forest);
      otherNodes.remove(referenceNode);

      otherNodes.sort(
          (n1, n2) -> {
            try {
              double similarity1 =
                  EmbeddingUtils.calculateSimilarity(
                      referenceNode.getEmbedding(), n1.getEmbedding());
              double similarity2 =
                  EmbeddingUtils.calculateSimilarity(
                      referenceNode.getEmbedding(), n2.getEmbedding());
              return Double.compare(similarity2, similarity1); // 按相似性降序排列
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });

      // 根据相似性逐步添加节点并计算累计相似性
      double cumulativeSimilarity = 1.0;
      for (TreeNode node : otherNodes) {
        double similarity =
            EmbeddingUtils.calculateSimilarity(referenceNode.getEmbedding(), node.getEmbedding());
        cumulativeSimilarity *= similarity;
        System.out.println("当前累计相似性: " + cumulativeSimilarity);

        if (cumulativeSimilarity > SIMILARITY_THRESHOLD) {
          selectedNodes.add(node);
        } else {
          break; // 达到阈值，停止继续合并
        }
      }

      // 合并满足条件的节点
      if (selectedNodes.size() > 1) {
        String newConceptName = LLMUtils.getConcept(selectedNodes);
        List<Double> newEmbedding = EmbeddingUtils.getEmbedding(newConceptName);
        TreeNode newParent = new TreeNode(newConceptName, newConceptName, newEmbedding);

        for (TreeNode node : selectedNodes) {
          newParent.getChildren().add(node);
          changePath(node, newConceptName);
        }

        // 从森林中移除已合并的节点，并加入新合并的节点
        forest.removeAll(selectedNodes);
        forest.add(newParent);

        // 重置连续失败计数器
        failureCount = 0;
        System.out.println("合并成功，新森林大小为: " + forest.size());
      } else {
        // 增加失败计数
        failureCount++;
        System.out.println("合并失败，连续失败次数: " + failureCount);
      }
    }

    if (failureCount >= MAX_CONTINUOUS_FAILURE_COUNT) {
      System.out.println("连续合并失败次数已达到上限，停止合并");
    }

    root.setChildren(forest);
  }

  private void changePath(TreeNode node, String prePath) {
    node.setPath(prePath + "." + node.getPath());
    for (TreeNode child : node.getChildren()) {
      changePath(child, prePath);
    }
  }
}
