package org.apache.zeppelin.iginx.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddingUtils {
  private static final Logger logger = LoggerFactory.getLogger(EmbeddingUtils.class);
  private static final Map<String, List<Double>> embeddings = new HashMap<>();
  private static final int EMBEDDING_DIMENSION = 50; // 嵌入向量维度
  private static final Random RANDOM = new Random(42); // 固定随机种子

  static {
    try {
      InputStream inputStream =
          EmbeddingUtils.class.getClassLoader().getResourceAsStream("model/glove.6B.50d.txt");
      if (inputStream == null) {
        throw new IllegalArgumentException("fail to find GloVe model");
      }

      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] tokens = line.split(" ");
          String word = tokens[0];
          List<Double> vector = new ArrayList<>(tokens.length - 1);
          for (int i = 1; i < tokens.length; i++) {
            vector.add(Double.parseDouble(tokens[i]));
          }
          embeddings.put(word, vector);
        }
      }
      logger.info("load GloVe success");
    } catch (Exception e) {
      throw new RuntimeException("load GloVe fail", e);
    }
  }

  /**
   * 获取输入内容的 embedding
   * 1. 直接通过 Map 查找，若找到，直接返回结果
   * 2. 若找不到，则对输入内容按照 "-", " ", "_" 进行划分，再分别获取 embeddig 并取平均
   * 3. 若输入的内容找不到且已无法再划分，则随机一个 embedding (固定了随机种子，为了让每次执行的结果一致)
   *
   * @param word
   * @return
   */
  public static List<Double> getEmbedding(String word) {
    List<Double> embedding = embeddings.get(word);

    if (embedding == null) {
      // 按照"-"," ","_"分割取embedding的平均值
      String[] parts = word.split("[-\\s_]+");
      if (parts.length > 1) {
        List<Double> sumEmbedding = new ArrayList<>();
        double sum = 0;
        for (String part : parts) {
          List<Double> partEmbedding = embeddings.get(part);
          if (partEmbedding != null) {
            for (int i = 0; i < EMBEDDING_DIMENSION; i++) {
              sumEmbedding.set(i, sumEmbedding.get(i) + partEmbedding.get(i)); // 按维度累加
            }
            sum += 1;
          }
        }
        for (int i = 0; i < EMBEDDING_DIMENSION; i++) {
          sumEmbedding.set(i, sumEmbedding.get(i) / sum);
        }
        return sumEmbedding;
      } else {
        // 如果所有部分都未找到且不可拆分，生成随机向量
        embedding = generateRandomVector(EMBEDDING_DIMENSION);
        logger.info("fail to find the embedding of '" + word + "', generating Random Vector to replace");
      }
    }
    return embedding;
  }

  private static List<Double> generateRandomVector(int dimension) {
    List<Double> vector = new ArrayList<>(dimension);
    for (int i = 0; i < dimension; i++) {
      vector.add(RANDOM.nextDouble() * 2 - 1); // 随机值范围 [-1, 1]
    }
    return vector;
  }

  public static double calculateSimilarity(List<Double> embedding1, List<Double> embedding2) {
    if (embedding1 == null || embedding2 == null || embedding1.size() != embedding2.size()) {
      throw new IllegalArgumentException(
          "Embeddings must not be null and must have the same length");
    }

    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;

    for (int i = 0; i < embedding1.size(); i++) {
      double valueA = embedding1.get(i);
      double valueB = embedding2.get(i);
      dotProduct += valueA * valueB;
      normA += Math.pow(valueA, 2);
      normB += Math.pow(valueB, 2);
    }

    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  public static void main(String[] args) {
    // 测试：获取两个句子的嵌入向量并计算相似度
    List<Double> embedding1 = getEmbedding("weather");
    List<Double> embedding2 = getEmbedding("climate");

    double similarity = calculateSimilarity(embedding1, embedding2);
    System.out.println("相似度: " + similarity);
  }
}
