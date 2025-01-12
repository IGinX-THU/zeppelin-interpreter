package org.apache.zeppelin.iginx.service;

import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.filter.PropertyFilter;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.zeppelin.iginx.dao.MilvusDao;
import org.apache.zeppelin.iginx.util.LLMUtils;
import org.apache.zeppelin.iginx.util.NetworkTreeNode;
import org.apache.zeppelin.iginx.util.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkService {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkService.class);
  private static final Double RELATION_THRESHOLD = 0.85; // 关系阈值
  private static final Integer RELATION_DEPTH_LEVEL = 3; // 关系深度层级
  private static final Integer MERGE_MIN_SIZE = 5; // 需要聚类的最小值
  private static final String MERGE_SQL_STR = "select merge(*, str='@@@') from (show columns ###);";
  private Boolean needMerge; // 是否需要合并
  private Boolean needRelation; // 是否需要计算关系
  private Session session;
  private String paragraphId;
  private List<List<String>> columnPath;
  private NetworkTreeNode root;
  private MilvusDao milvusDao;
  private Map<String, Map<String, Relation>> relationMap = new ConcurrentHashMap<>();

  public NetworkService(
      Boolean needMerge,
      Boolean needRelation,
      String paragraphId,
      List<List<String>> columnPath,
      Session session) {
    this.needMerge = needMerge;
    this.needRelation = needRelation;
    this.paragraphId = paragraphId;
    this.columnPath = columnPath;
    this.session = session;
    if (needRelation) {
      this.milvusDao = MilvusDao.getInstance();
    }
  }

  public String initNetwork() {
    LOGGER.info("initNetwork: {} {} {}", needMerge, needRelation, paragraphId);
    root = new NetworkTreeNode("rootId", "数据资产", 0);
    buildForest(root, columnPath);
    if (needMerge) {
      LOGGER.info("before merge, the size is：{}", root.getChildren().size());
      mergeForest(root);
      LOGGER.info("after merge, the size is：{}", root.getChildren().size());
    }
    List<NetworkTreeNode> nodeList = new ArrayList<>();
    nodeList.add(root);
    root.setExpanded(true);
    root.setShown(true);
    for (NetworkTreeNode childNode : root.getChildren().values()) {
      childNode.setShown(true);
      nodeList.add(childNode);
    }
    PropertyFilter filter =
        (Object object, String name, Object value) -> {
          return "id".equals(name) || "name".equals(name) || "depth".equals(name);
        };
    String nodeString = JSON.toJSONString(nodeList, filter);
    LOGGER.info("the nodeString is {}", nodeString);

    String relationString = "";
    if (needRelation) {
      addEmbedding(root);
      calculateNodeRelation(root);
      List<Relation> relationList = analyseNodeRelation();
      relationString = JSON.toJSONString(relationList);
      LOGGER.info("the relationString is {}", relationString);
    }
    if (relationString.isEmpty()) {
      LOGGER.info("relationString is empty");
      relationString = "[]";
    }
    return loadHtmlTemplate()
        .replace("NODE_LIST", nodeString)
        .replace("RELATION_LIST", relationString);
  }

  public String handleNodeClick(String nodeId) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("handleNodeClick");
    NetworkTreeNode node = getNodeById(nodeId);
    if (node == null) {
      LOGGER.error("Node not found for id: {}", nodeId);
      return "{}";
    }

    JSONObject result = new JSONObject();
    JSONObject addMap = new JSONObject();
    JSONObject removeMap = new JSONObject();

    if (node.getExpanded()) {
      collapseNode(node, addMap, removeMap);
      node.setExpanded(false);
    } else {
      expandNode(node, addMap);
      node.setExpanded(true);
    }

    result.put("add", addMap);
    result.put("remove", removeMap);
    long endTime = System.currentTimeMillis();
    LOGGER.info("handleNodeClick run time：" + (endTime - startTime) + "ms");
    return result.toString();
  }

  // todo:后续改为多线程并行
  // todo:数据量很大时，考虑先只build前几层？
  private void buildForest(NetworkTreeNode root, List<List<String>> columnPath) {
    long startTime = System.currentTimeMillis();
    for (int i = 1; i < columnPath.size(); i++) {
      List<String> path = columnPath.get(i);
      String pathString = path.get(0);
      String[] pathParts = pathString.split("\\.");
      NetworkTreeNode currentNode = root;
      for (String nodeName : pathParts) {
        NetworkTreeNode childNode = currentNode.getChildren().get(nodeName);
        if (childNode == null) {
          childNode =
              new NetworkTreeNode(
                  currentNode.getId() + "." + nodeName, // 生成节点ID
                  nodeName, // 节点名称
                  currentNode.getDepth() + 1 // 父节点深度 + 1
                  );
          currentNode.getChildren().put(nodeName, childNode);
        }
        currentNode = childNode;
      }
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("buildForest run time：" + (endTime - startTime) + "ms");
  }

  // todo:并行优化
  // todo:数据量很大时，updateNodes会几乎遍历所有结点，比较耗时，后续考虑借鉴懒标记思想优化？
  private void mergeForest(NetworkTreeNode root) {
    long startTime = System.currentTimeMillis();
    JSONArray jsonArray = new JSONArray();
    for (NetworkTreeNode childNode : root.getChildren().values()) {
      jsonArray.add(childNode.getName());
    }
    if (jsonArray.size() < MERGE_MIN_SIZE) {
      LOGGER.info("the size of the forest is too small");
      return;
    }
    String str = "";
    try {
      str = jsonArray.toString();
    } catch (Exception e) {
      LOGGER.error("JSON fail", e);
    }

    String sql = MERGE_SQL_STR.replace("@@@", str);
    List<List<String>> queryList = getQueryList(sql);
    if (queryList == null) {
      return;
    }

    // 根据label区分
    Map<String, List<NetworkTreeNode>> labelToNodesMap = new HashMap<>();
    for (int i = 1; i < queryList.size(); i++) {
      List<String> row = queryList.get(i);
      String label = row.get(0);
      LOGGER.info("label: {}", label);
      String nodeName = row.get(1);
      LOGGER.info("nodeName: {}", nodeName);
      NetworkTreeNode node = root.getChildren().get(nodeName);
      if (node != null) {
        labelToNodesMap.computeIfAbsent(label, k -> new ArrayList<>()).add(node);
      }
    }
    LOGGER.info("the size of labelToNodesMap is {}", labelToNodesMap.size());

    // 对每个label进行合并
    for (Map.Entry<String, List<NetworkTreeNode>> entry : labelToNodesMap.entrySet()) {
      List<NetworkTreeNode> nodesToMerge = entry.getValue();
      if (nodesToMerge.size() > 1) {
        String mergedName = LLMUtils.getConcept(nodesToMerge);
        NetworkTreeNode mergedNode = new NetworkTreeNode("rootId." + mergedName, mergedName, 1);
        for (NetworkTreeNode node : nodesToMerge) {
          mergedNode.getChildren().put(node.getName(), node);
          updateNodes(node, mergedNode.getName());
          root.getChildren().remove(node.getName());
        }
        root.getChildren().put(mergedNode.getName(), mergedNode);
      }
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("mergeForest run time：" + (endTime - startTime) + "ms");
  }

  private List<List<String>> getQueryList(String sql) {
    List<List<String>> queryList = null;
    try {
      SessionExecuteSqlResult sqlResult = session.executeSql(sql);
      queryList = sqlResult.getResultInList(false, FormatUtils.DEFAULT_TIME_FORMAT, "");
    } catch (Exception e) {
      LOGGER.info("encounter error when executing sql statement:\n" + e.getMessage());
    }
    if (queryList == null || queryList.size() <= 1) {
      LOGGER.info("Invalid queryList or insufficient data, the sql is {}", sql);
      return null;
    }
    LOGGER.info("the size of queryList is {}", queryList.size());
    return queryList;
  }

  private void updateNodes(NetworkTreeNode node, String mergeRoot) {
    node.setMergedRoot(mergeRoot);
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      updateNodes(childNode, mergeRoot);
    }
  }

  private String loadHtmlTemplate() {
    String htmlTemplate = "static/vis/network.html";
    try (InputStream inputStream =
        this.getClass().getClassLoader().getResourceAsStream(htmlTemplate)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      StringBuilder content = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }
      return content.toString();
    } catch (IOException e) {
      LOGGER.warn("load show columns to network error", e);
    }
    return htmlTemplate;
  }

  private NetworkTreeNode getNodeById(String nodeId) {
    String[] ids = nodeId.split("\\.");
    NetworkTreeNode currentNode = root;
    for (int i = 1; i < ids.length; i++) {
      currentNode = currentNode.getChildren().get(ids[i]);
      if (currentNode == null) return null;
    }
    return currentNode;
  }

  private void expandNode(NetworkTreeNode node, JSONObject addMap) {
    JSONArray nodes = new JSONArray();
    JSONArray edges = new JSONArray();
    JSONArray links = new JSONArray();
    if (needRelation) {
      addEmbedding(node);
      calculateNodeRelation(node);
      List<Relation> addRelations = analyseNodeRelation();
      links = getRelationLinks(addRelations);
    }

    for (NetworkTreeNode child : node.getChildren().values()) {
      child.setShown(true);
      JSONObject nodeData = new JSONObject();
      nodeData.put("id", child.getNetworkId());
      nodeData.put("name", child.getName());
      nodeData.put("depth", child.getDepth());
      nodes.add(nodeData);

      JSONObject edgeData = new JSONObject();
      edgeData.put("from", node.getNetworkId());
      edgeData.put("to", child.getNetworkId());
      edges.add(edgeData);
    }

    addMap.put("nodes", nodes);
    addMap.put("edges", edges);
    addMap.put("links", links);
  }

  private void collapseNode(NetworkTreeNode node, JSONObject addMap, JSONObject removeMap) {
    JSONArray nodes = new JSONArray();
    for (NetworkTreeNode child : node.getChildren().values()) {
      collectShownNodes(child, nodes);
    }
    removeMap.put("nodes", nodes);

    if (needRelation) {
      List<Relation> addRelations = analyseNodeRelation();
      JSONArray links = getRelationLinks(addRelations);
      addMap.put("links", links);
    }
  }

  private JSONArray getRelationLinks(List<Relation> addRelations) {
    JSONArray links = new JSONArray();
    for (Relation relation : addRelations) {
      JSONObject relationJson = new JSONObject();
      relationJson.put("from", relation.getFrom());
      relationJson.put("to", relation.getTo());
      relationJson.put("relation", relation.getRelation());
      links.add(relationJson);
    }
    return links;
  }

  private void collectShownNodes(NetworkTreeNode node, JSONArray nodes) {
    if (node.getShown()) {
      JSONObject nodeData = new JSONObject();
      nodeData.put("id", node.getNetworkId());
      nodes.add(nodeData);
      node.setShown(false);
      for (NetworkTreeNode child : node.getChildren().values()) {
        collectShownNodes(child, nodes); // 递归处理
      }
    }
  }

  private void addEmbedding(NetworkTreeNode node) {
    LOGGER.info("begin addEmbedding for {}'s children", node.getId());
    List<String> paths = new ArrayList<>();
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      paths.add(childNode.getEmbeddingId());
    }
    Map<String, List<Float>> result = milvusDao.queryEmbeddingByPaths(paths);
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      childNode.setEmbedding(result.get(childNode.getEmbeddingId()));
    }
  }

  private void calculateNodeRelation(NetworkTreeNode node) {
    LOGGER.info("calculateNodeRelation: nodeId is {}", node.getId());
    if (node.getDepth() >= RELATION_DEPTH_LEVEL) return;
    List<NetworkTreeNode> visibleNodes = getVisibleNodes();
    LOGGER.info("the size of visibleNodes is {}", visibleNodes.size());
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      if (childNode.getEmbedding() == null) {
        LOGGER.info("the embedding of node is null: {}", childNode.getId());
        continue;
      }
      for (NetworkTreeNode visibleNode : visibleNodes) {
        if (visibleNode.getEmbedding() == null) {
          LOGGER.info("the embedding of visibleNode is null: {}", visibleNode.getId());
          continue;
        }
        String rootId1 = getRootId(childNode.getNetworkId());
        String rootId2 = getRootId(visibleNode.getNetworkId());
        if (StringUtils.isEqual(rootId1, rootId2)) {
          continue;
        }

        String bucket = getKeyWithIds(rootId1, rootId2);
        if (!this.relationMap.containsKey(bucket)) {
          Map<String, Relation> map = new HashMap<>();
          this.relationMap.put(bucket, map);
        }

        String key = getKeyWithIds(childNode.getNetworkId(), visibleNode.getNetworkId());
        Map<String, Relation> bucketMap = this.relationMap.get(bucket);
        if (!bucketMap.containsKey(key)) {
          Double score = cosineSimilarity(childNode.getEmbedding(), visibleNode.getEmbedding());
          Relation relation =
              new Relation(childNode.getNetworkId(), visibleNode.getNetworkId(), score);
          bucketMap.put(key, relation);
        }
      }
    }
    LOGGER.info("now relationMap is {}", relationMap);
  }

  private List<Relation> analyseNodeRelation() {
    LOGGER.info("analyseNodeRelation");
    // 使用并行流来处理 relationMap
    List<Relation> addRelations =
        relationMap
            .values()
            .parallelStream()
            .map(
                innerMap -> {
                  Relation bestRelation =
                      innerMap.values().stream()
                          .max(Comparator.comparingDouble(Relation::getScore))
                          .orElse(null);
                  if (bestRelation != null && bestRelation.getScore() > RELATION_THRESHOLD) {
                    if (bestRelation.getRelation().isEmpty()) {
                      String[] parts1 = bestRelation.getFrom().split("\\.");
                      String[] parts2 = bestRelation.getTo().split("\\.");
                      String name1 = parts1[parts1.length - 1];
                      String name2 = parts2[parts2.length - 1];
                      String relation = LLMUtils.getRelation(name1, name2);
                      bestRelation.setRelation(relation);
                    }
                    return bestRelation;
                  }
                  return null;
                })
            .filter(Objects::nonNull) // 过滤掉 null 值
            .collect(Collectors.toList());
    LOGGER.info("finish analyseNodeRelation, the size of links is {}", addRelations.size());
    return addRelations;
  }

  public static String getRootId(String str) {
    String[] parts = str.split("\\.");
    if (parts.length < 2) {
      throw new IllegalArgumentException("输入字符串格式不正确");
    }
    return parts[0] + "." + parts[1];
  }

  public static String getKeyWithIds(String a, String b) {
    return a.compareTo(b) < 0 ? a + "-" + b : b + "-" + a;
  }

  private Double cosineSimilarity(List<Float> embedding1, List<Float> embedding2) {
    if (embedding1 == null || embedding2 == null) {
      throw new IllegalArgumentException("embedding不能为空");
    }
    if (embedding1.size() != embedding2.size()) {
      throw new IllegalArgumentException("两个向量的维度必须相同");
    }
    // 点积计算
    double dotProduct =
        IntStream.range(0, embedding1.size())
            .parallel()
            .mapToDouble(i -> embedding1.get(i) * embedding2.get(i))
            .sum();
    // 向量模长计算
    double magnitude1 = Math.sqrt(embedding1.parallelStream().mapToDouble(val -> val * val).sum());
    double magnitude2 = Math.sqrt(embedding2.parallelStream().mapToDouble(val -> val * val).sum());
    return dotProduct / (magnitude1 * magnitude2);
  }

  private List<NetworkTreeNode> getVisibleNodes() {
    List<NetworkTreeNode> result = new ArrayList<>();
    for (NetworkTreeNode node : root.getChildren().values()) {
      getVisibleNodes(node, result);
    }
    return result;
  }

  private void getVisibleNodes(NetworkTreeNode node, List<NetworkTreeNode> result) {
    if (node.getShown()) {
      result.add(node);
      for (NetworkTreeNode childNode : node.getChildren().values()) {
        getVisibleNodes(childNode, result);
      }
    }
  }
}
