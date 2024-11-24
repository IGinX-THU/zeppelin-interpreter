package org.apache.zeppelin.iginx.util.algorithm.mergeforest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.zeppelin.iginx.util.EmbeddingUtils;
import org.apache.zeppelin.iginx.util.LLMUtils;
import org.apache.zeppelin.iginx.util.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomMerge implements MergeForestStrategy {
  private static final Logger logger = LoggerFactory.getLogger(RandomMerge.class);
  private static final Double SIMILARITY_THRESHOLD = 0.7;
  private static final Integer MAX_CONTINUOUS_FAILURE_COUNT = 5;

  /**
   * 随机合并算法 1. 循环多轮进行，当某轮结束后的森林大小满足要求或者已经连续 MAX_CONTINUOUS_FAILURE_COUNT 轮合并失败，则退出 2.
   * 每轮随机选择森林中的一棵树，计算其他所有树与它的 embedding 相似度，并从高到底排序 3. 按照顺序对相似度进行累乘：假设相似度是 x，每次乘实际上是乘 1-(1-x)/2
   * ，乘积如果不小于 SIMILARITY_THRESHOLD 则继续乘，反之则结束 4. 如果累计相似度满足要求的树不少于2棵，则进行合并，反之则计为一次合并失败
   *
   * @param root
   */
  @Override
  public void mergeForest(TreeNode root) {
    List<TreeNode> forest = root.getChildren();
    int targetTreeCount = (int) Math.ceil(Math.sqrt(forest.size()));
    Random random = new Random();
    int failureCount = 0; // 记录连续合并失败次数

    while (forest.size() > targetTreeCount && failureCount < MAX_CONTINUOUS_FAILURE_COUNT) {
      logger.info("beginning new merge, the recent size of forest is: {}", forest.size());

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
        cumulativeSimilarity *= (0.5 + similarity / 2);
        logger.info("now cumulative similarity: {}", cumulativeSimilarity);

        if (cumulativeSimilarity >= SIMILARITY_THRESHOLD) {
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
        logger.info("merge success, now forest size is: {}", forest.size());
      } else {
        // 增加失败计数
        failureCount++;
        logger.info("merge failure, now consecutive failure count is: {}", failureCount);
      }

      // 判断是否已经到达连续合并失败的最大上限
      if (failureCount >= MAX_CONTINUOUS_FAILURE_COUNT) {
        logger.info("The count of consecutive merge failures has reached the limit, stop merge");
        break;
      }
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
