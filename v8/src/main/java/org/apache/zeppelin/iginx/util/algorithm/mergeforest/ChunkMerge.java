package org.apache.zeppelin.iginx.util.algorithm.mergeforest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.zeppelin.iginx.util.EmbeddingUtils;
import org.apache.zeppelin.iginx.util.LLMUtils;
import org.apache.zeppelin.iginx.util.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkMerge implements MergeForestStrategy {
  private static final Logger logger = LoggerFactory.getLogger(ChunkMerge.class);
  private static final int THREAD_POOL_SIZE = 8; // 可根据机器配置调整线程数

  /**
   * 分块合并算法 1. 循环多轮进行，当某轮结束后的森林大小满足要求，则退出 2. 每轮按照线程数平均分块，每块中的树分别进行合并，块间互不干扰 3. 通过 findBestMergeGroup
   * 查找最合适的合并组：块中的所有树通过循环，两两比较，找到 embedding 最相似的两个合并 (第3步的 findBestMergeGroup
   * 有较大的优化空间，因为有时候多棵树一起合并可能效果更好)
   *
   * @param root
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void mergeForest(TreeNode root) throws ExecutionException, InterruptedException {
    List<TreeNode> forest = root.getChildren();
    int targetTreeCount = (int) Math.ceil(Math.sqrt(forest.size()));
    logger.info("targetTreeCount: {}", targetTreeCount);
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    while (forest.size() > targetTreeCount) {
      logger.info("beginning new merge, the recent size of forest is: {}", forest.size());
      List<Future<List<TreeNode>>> futureResults = new ArrayList<>();

      int chunkSize = Math.max(2, (int) Math.ceil((double) forest.size() / THREAD_POOL_SIZE));
      System.out.println("chunkSize: " + chunkSize);
      for (int i = 0; i < forest.size(); i += chunkSize) {
        List<TreeNode> chunk =
            new ArrayList<>(forest.subList(i, Math.min(i + chunkSize, forest.size())));
        futureResults.add(executor.submit(() -> mergeChunk(chunk)));
      }

      List<TreeNode> mergedForest = new ArrayList<>();
      for (Future<List<TreeNode>> future : futureResults) {
        mergedForest.addAll(future.get());
      }
      forest.clear();
      forest.addAll(mergedForest);

      if (forest.size() <= targetTreeCount) {
        break;
      }
    }
    executor.shutdown();

    root.setChildren(forest);
  }

  private List<TreeNode> mergeChunk(List<TreeNode> chunk) {
    if (chunk.size() == 1) return chunk;
    List<TreeNode> mergedChunk = new ArrayList<>(chunk);

    List<TreeNode> nodesToMerge = findBestMergeGroup(mergedChunk);

    if (!nodesToMerge.isEmpty()) {
      String newConceptName = LLMUtils.getConcept(nodesToMerge);
      logger.info("get new concept name: {}", newConceptName);

      List<Double> newEmbedding = EmbeddingUtils.getEmbedding(newConceptName);
      TreeNode newParent = new TreeNode(newConceptName, newConceptName, newEmbedding);

      for (TreeNode node : nodesToMerge) {
        newParent.getChildren().add(node);
        changePath(node, newConceptName);
      }

      mergedChunk.removeAll(nodesToMerge);
      mergedChunk.add(newParent);
    }

    return mergedChunk;
  }

  private List<TreeNode> findBestMergeGroup(List<TreeNode> nodes) {
    List<TreeNode> bestGroup = new ArrayList<>();
    double bestSimilarity = -1;

    for (int i = 0; i < nodes.size(); i++) {
      for (int j = i + 1; j < nodes.size(); j++) {
        double similarity =
            EmbeddingUtils.calculateSimilarity(
                nodes.get(i).getEmbedding(), nodes.get(j).getEmbedding());
        if (similarity > bestSimilarity) {
          bestSimilarity = similarity;
          bestGroup = Arrays.asList(nodes.get(i), nodes.get(j));
        }
      }
    }

    return bestGroup;
  }

  private void changePath(TreeNode node, String prePath) {
    node.setPath(prePath + "." + node.getPath());
    for (TreeNode child : node.getChildren()) {
      changePath(child, prePath);
    }
  }
}
