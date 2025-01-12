package org.apache.zeppelin.iginx.dao;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.QueryParam;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusDao.class);
  // todo:将ip和port修改到从配置文件中获取
  private String milvusHost = "localhost"; // 默认 IP 地址
  private Integer milvusPort = 19530; // 默认端口
  private static final Integer DIMENSION = 768; // 维度
  private static volatile MilvusDao instance; // 单例实例
  private MilvusServiceClient milvusServiceClient;

  private MilvusDao(String milvusHost, Integer milvusPort) {
    this.milvusHost = milvusHost;
    this.milvusPort = milvusPort;
    LOGGER.info("milvusHost is: {}, milvusPort is: {}", milvusHost, milvusPort);
    this.milvusServiceClient = createClient();
  }

  /**
   * 获取单例实例（线程安全，双重检查锁）
   *
   * @return 单例实例
   */
  public static MilvusDao getInstance(String milvusHost, Integer milvusPort) {
    if (instance == null) {
      synchronized (MilvusDao.class) {
        if (instance == null) {
          instance = new MilvusDao(milvusHost, milvusPort);
        }
      }
    }
    return instance;
  }

  /**
   * 构造 Milvus 客户端并进行连接
   *
   * @return 返回创建的 Milvus 客户端
   */
  private MilvusServiceClient createClient() {
    ConnectParam connectParam =
        ConnectParam.newBuilder().withHost(milvusHost).withPort(milvusPort).build();

    milvusServiceClient = new MilvusServiceClient(connectParam);
    LOGGER.info("已连接Milvus数据库");
    return milvusServiceClient;
  }

  /**
   * 获取已经创建的 Milvus 客户端
   *
   * @return Milvus 客户端
   */
  public MilvusServiceClient getClient() {
    if (milvusServiceClient == null) {
      throw new IllegalStateException("Milvus client has not been initialized.");
    }
    return milvusServiceClient;
  }

  /** 关闭 Milvus 客户端连接 */
  public void close() {
    if (milvusServiceClient != null) {
      try {
        milvusServiceClient.close();
      } catch (Exception e) {
        LOGGER.error("Error closing Milvus client", e);
      }
    }
  }

  /**
   * 根据单个字符串路径查询对应的 embedding 向量
   *
   * @param path 输入的路径
   * @return 返回查询到的 embedding 向量
   */
  public List<Float> queryEmbeddingByPath(String path) {
    if (milvusServiceClient == null) {
      throw new IllegalStateException("Milvus client has not been initialized.");
    }
    String expr = "path == \"" + path + "\"";
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName("data_embedding")
            .withExpr(expr)
            .withOutFields(Collections.singletonList("embedding")) // 只返回 embedding 字段
            .build();

    R<QueryResults> response = milvusServiceClient.query(queryParam);
    if (response.getStatus() == R.Status.Success.getCode()) {
      QueryResults queryResults = response.getData();
      return queryResults
          .getFieldsDataList()
          .get(0)
          .getVectors()
          .getFloatVectorOrBuilder()
          .getDataList();
    } else {
      throw new RuntimeException("Query failed with status code: " + response.getStatus());
    }
  }

  /**
   * 批量查询多个路径的 embedding 向量
   *
   * @param paths 输入的路径列表
   * @return 返回一个 Map，键是路径，值是对应的 embedding 向量列表
   */
  public Map<String, List<Float>> queryEmbeddingByPaths(List<String> paths) {
    if (milvusServiceClient == null) {
      throw new IllegalStateException("Milvus client has not been initialized.");
    }

    Map<String, List<Float>> resultMap = new HashMap<>();

    // 构建查询表达式
    String expr =
        paths.stream().map(path -> "path == \"" + path + "\"").collect(Collectors.joining(" || "));
    LOGGER.info("expr is: {}", expr);

    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName("data_embedding")
            .withExpr(expr) // 使用构建的查询表达式
            .withOutFields(Arrays.asList("path", "embedding"))
            .build();

    R<QueryResults> response = milvusServiceClient.query(queryParam);

    if (response.getStatus() == R.Status.Success.getCode()) {
      QueryResults queryResults = response.getData();
      List<FieldData> fieldsDataList = queryResults.getFieldsDataList();

      List<List<Float>> embeddingResult = new ArrayList<>();
      List<String> pathResult = new ArrayList<>();
      for (FieldData fieldData : fieldsDataList) {
        if (fieldData.getFieldName().equals("embedding")) {
          // 提取 embedding 数据
          List<Float> embeddingData =
              fieldData.getVectors().getFloatVectorOrBuilder().getDataList();
          LOGGER.info("The size of embeddingData is {}", embeddingData.size());
          for (int i = 0; i < embeddingData.size(); i += DIMENSION) {
            List<Float> subList = embeddingData.subList(i, i + DIMENSION);
            embeddingResult.add(subList);
          }
        } else if (fieldData.getFieldName().equals("path")) {
          // 提取 path 数据
          pathResult = fieldData.getScalars().getStringData().getDataList();
        }
      }
      if (pathResult.size() != embeddingResult.size()) {
        LOGGER.warn("queryEmbeddingByPaths size mismatch: pathResult != embeddingResult");
      }
      for (int i = 0; i < pathResult.size(); i++) {
        resultMap.put(pathResult.get(i), embeddingResult.get(i));
      }
    } else {
      throw new RuntimeException("Query failed with status code: " + response.getStatus());
    }

    return resultMap;
  }
}
