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

public class ChunkMerge implements MergeForestStrategy {
  private static final int THREAD_POOL_SIZE = 8; // 可根据机器配置调整线程数

  public void mergeForest(TreeNode root) throws ExecutionException, InterruptedException {
    List<TreeNode> forest = root.getChildren();
    int targetTreeCount = (int) Math.ceil(Math.sqrt(forest.size()));
    System.out.println("targetTreeCount: " + targetTreeCount);
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    while (forest.size() > targetTreeCount) {
      System.out.println("开始新一轮合并，当前森林大小: " + forest.size());
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
    System.out.println("mergeChunk");
    if (chunk.size() == 1) return chunk;
    List<TreeNode> mergedChunk = new ArrayList<>(chunk);

    List<TreeNode> nodesToMerge = findBestMergeGroup(mergedChunk);

    if (!nodesToMerge.isEmpty()) {
      String newConceptName = LLMUtils.getConcept(nodesToMerge);
      System.out.println("newConceptName: " + newConceptName);

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
    System.out.println("findBestMergeGroup");
    List<TreeNode> bestGroup = new ArrayList<>();
    double bestSimilarity = -1;

    for (int i = 0; i < nodes.size(); i++) {
      for (int j = i + 1; j < nodes.size(); j++) {
        double similarity =
            EmbeddingUtils.calculateSimilarity(
                nodes.get(i).getEmbedding(), nodes.get(j).getEmbedding());
        if (similarity > bestSimilarity) {
          bestSimilarity = similarity;
          bestGroup = Arrays.asList(nodes.get(i), nodes.get(j)); // 使用 Arrays.asList 替代 List.of
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
