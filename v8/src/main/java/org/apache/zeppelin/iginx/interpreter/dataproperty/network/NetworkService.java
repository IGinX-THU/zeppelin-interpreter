package org.apache.zeppelin.iginx.interpreter.dataproperty.network;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import java.util.*;
import org.apache.velocity.VelocityContext;
import org.apache.zeppelin.iginx.interpreter.IginxDao;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.ClusterNode;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.Relation;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.SearchedNode;
import org.apache.zeppelin.iginx.util.VelocityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkService {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkService.class);
  private static final Integer RELATION_DEPTH_LEVEL = 3; // 关系深度层级
  private static final Integer MERGE_MIN_SIZE = 5; // 需要聚类的最小值
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private Boolean needMerge; // 是否需要合并
  private Boolean needRelation; // 是否需要计算关系
  private String paragraphId;
  private List<String[]> columnPath;
  private NetworkTreeNode root;
  private IginxDao iginx;
  private final String pattern;
  private static final Map<String, String> embeddingId2NetworkId = new HashMap<>();
  private static final String DEFAULT_RELATION_FUNCTION = "analyse_relation";

  public NetworkService(
      Boolean needMerge,
      Boolean needRelation,
      String paragraphId,
      List<String[]> columnPath,
      IginxDao iginx,
      String pattern) {
    this.needMerge = needMerge;
    this.needRelation = needRelation;
    this.paragraphId = paragraphId;
    this.columnPath = columnPath;
    this.iginx = iginx;
    this.pattern = Objects.requireNonNull(pattern);
  }

  public String initNetwork(VelocityContext velocityContext) {
    LOGGER.info("initNetwork: {} {} {}", needMerge, needRelation, paragraphId);
    root = new NetworkTreeNode("rootId", "Data Asset", 0);
    buildForest(root, columnPath);
    if (needMerge) {
      LOGGER.info("before merge, the size is：{}", root.getChildren().size());
      mergeForest(root);
      LOGGER.info("after merge, the size is：{}", root.getChildren().size());
    }
    JSONArray nodes = getNodesData(root);
    String nodeString =
        nodes.toJSONString().replace("'", "\\'").replace("\\\"", "").replace("\\n", " ");
    LOGGER.info("the nodeString is {}", nodeString);

    String relationString = "";
    if (needRelation && !needMerge) {
      List<Relation> relationList = calculateNodeRelation(root, DEFAULT_RELATION_FUNCTION);
      relationString = JSON.toJSONString(relationList);
      LOGGER.info("the relationString is {}", relationString);
    }
    if (relationString.isEmpty()) {
      LOGGER.info("relationString is empty");
      relationString = "[]";
    }

    velocityContext.put("nodeList", nodeString);
    velocityContext.put("relationList", relationString);
    return VelocityUtil.generate("templates/data-property.vm", velocityContext);
  }

  public String handleNodeClick(String nodeId, String function) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("handleNodeClick");
    NetworkTreeNode node = getNodeById(nodeId);
    if (node == null) {
      LOGGER.error("Node not found for id: {}", nodeId);
      return "{}";
    }

    JSONObject result = new JSONObject();
    JSONObject addMap = new JSONObject();
    //    JSONObject removeMap = new JSONObject();

    if (node.getExpanded()) {
      throw new IllegalStateException("Node is already expanded");
      //      collapseNode(node, addMap, removeMap);
      //      node.setExpanded(false);
    } else {
      expandNode(node, addMap, function);
      node.setExpanded(true);
    }

    result.put("add", addMap);
    //    result.put("remove", removeMap);
    long endTime = System.currentTimeMillis();
    LOGGER.info("handleNodeClick run time：" + (endTime - startTime) + "ms");
    return result.toString();
  }

  // todo:数据量很大时，考虑先只build前几层？
  private void buildForest(NetworkTreeNode root, List<String[]> columnPath) {
    long startTime = System.currentTimeMillis();
    // 使用并行流处理 columnPath
    columnPath
        .parallelStream()
        .forEach(
            path -> {
              NetworkTreeNode currentNode = root;

              for (String nodeName : path) {
                // 使用同步块来保证线程安全
                synchronized (currentNode) {
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
            });

    long endTime = System.currentTimeMillis();
    LOGGER.info("buildForest run time：" + (endTime - startTime) + "ms");
  }

  // todo:数据量很大时，updateNodes会几乎遍历所有结点，比较耗时，后续考虑借鉴懒标记思想优化？
  private void mergeForest(NetworkTreeNode root) {
    if (root.getChildren().size() < MERGE_MIN_SIZE) {
      LOGGER.info("the size of the groupingMap is too small");
      return;
    }
    Multimap<ClusterNode, String> groupingMap =
        iginx.getGroupingOf(pattern, "default_fetch", "default_cluster", 18);

    Map<String, List<NetworkTreeNode>> labelToNodesMap = new HashMap<>();
    for (Map.Entry<ClusterNode, Collection<String>> group : groupingMap.asMap().entrySet()) {
      List<NetworkTreeNode> nodesToMerge = new ArrayList<>();
      for (String nodeName : group.getValue()) {
        NetworkTreeNode node = root.getChildren().get(nodeName);
        if (node == null) {
          throw new IllegalStateException("Node not found for name: " + nodeName);
        }
        nodesToMerge.add(node);
      }
      labelToNodesMap.put(group.getKey().getPath(), nodesToMerge);
    }

    root.getChildren().clear();
    for (Map.Entry<String, List<NetworkTreeNode>> clusterAndChildren : labelToNodesMap.entrySet()) {
      String cluster = clusterAndChildren.getKey();
      List<NetworkTreeNode> children = clusterAndChildren.getValue();
      String[] clusterParts = cluster.split("\\.");

      StringBuilder idBuilder = new StringBuilder("rootId.");
      NetworkTreeNode parent = root;
      for (int i = 0; i < clusterParts.length; i++) {
        int finalI = i;
        String clusterName = clusterParts[i];
        idBuilder.append(clusterName.replace("[^a-zA-Z0-9]", "_"));
        parent =
            parent
                .getChildren()
                .computeIfAbsent(
                    clusterName,
                    name ->
                        new NetworkTreeNode(idBuilder.toString(), clusterName, finalI + 1, true));
        idBuilder.append(".");
      }

      for (NetworkTreeNode child : children) {
        parent.getChildren().put(child.getName(), child);
        updateNodes(child, cluster);
      }
    }
  }

  private void updateNodes(NetworkTreeNode node, String mergeRoot) {
    node.setMergedRoot(mergeRoot);
    embeddingId2NetworkId.put(node.getEmbeddingId(), node.getNetworkId());
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      updateNodes(childNode, mergeRoot);
    }
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

  private void expandNode(NetworkTreeNode node, JSONObject addMap, String function) {
    JSONArray nodes = new JSONArray();
    JSONArray links = new JSONArray();
    if (needRelation) {
      List<Relation> addRelations = calculateNodeRelation(node, function);
      links = getRelationLinks(addRelations);
    }
    nodes = getNodesData(node);
    addMap.put("nodes", nodes);
    addMap.put("links", links);
  }

  private JSONArray getNodesData(NetworkTreeNode node) {
    JSONArray nodes = new JSONArray();
    if (node == root) {
      node.setShown(true);
      node.setExpanded(true);
      nodes.add(createNodeData(node));
    }
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      childNode.setShown(true);
      nodes.add(createNodeData(childNode));
    }
    return nodes;
  }

  private JSONObject createNodeData(NetworkTreeNode node) {
    JSONObject nodeData = new JSONObject();
    nodeData.put("id", node.getNetworkId());
    nodeData.put("name", node.getName());
    nodeData.put("depth", node.getDepth());
    nodeData.put("merge", node.isMergedNode());
    return nodeData;
  }

  private JSONArray getRelationLinks(List<Relation> addRelations) {
    JSONArray links = new JSONArray();
    for (Relation relation : addRelations) {
      JSONObject relationJson = new JSONObject();
      relationJson.put("from", relation.getFromPath());
      relationJson.put("to", relation.getToPath());
      relationJson.put("relation", relation.getDescription());
      relationJson.put("score", relation.getScore());
      links.add(relationJson);
    }
    return links;
  }

  private List<Relation> calculateNodeRelation(NetworkTreeNode node, String function) {
    LOGGER.info("calculateNodeRelation: nodeId is {}", node.getId());
    if (node.getDepth() >= RELATION_DEPTH_LEVEL) return new ArrayList<>();
    List<NetworkTreeNode> visibleNodes = getVisibleNodes();
    LOGGER.info("the size of visibleNodes is {}", visibleNodes.size());

    List<String> sourcePaths = new ArrayList<>();
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      if (childNode.isMergedNode()) {
        continue;
      }
      sourcePaths.add(childNode.getEmbeddingId());
    }

    List<String> targetPaths = new ArrayList<>();
    for (NetworkTreeNode visibleNode : visibleNodes) {
      if (visibleNode.isMergedNode()) {
        continue;
      }
      targetPaths.add(visibleNode.getEmbeddingId());
    }

    List<Relation> addRelations = iginx.analyseMainRelation(sourcePaths, targetPaths, function);
    List<Relation> relationsWithNetworkId = new ArrayList<>();

    for (Relation relation : addRelations) {
      String sourceEmbeddingId = relation.getFromPath();
      String targetEmbeddingId = relation.getToPath();
      double score = relation.getScore();

      String from = embeddingId2NetworkId.get(sourceEmbeddingId);
      String to = embeddingId2NetworkId.get(targetEmbeddingId);

      relationsWithNetworkId.add(new Relation(from, to, score, relation.getDescription()));
    }

    return relationsWithNetworkId;
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

  public String handleSearch(String keywords, String topK, String function) {
    List<SearchedNode> paths = iginx.search(pattern, keywords, topK, function);
    JSONArray searchResultJson = new JSONArray();
    for (SearchedNode pathWithScore : paths) {
      String path = pathWithScore.getPath();
      String description = pathWithScore.getDescription();
      double score = pathWithScore.getScore();
      String networkId = embeddingId2NetworkId.get(path);
      if (networkId != null) {
        JSONObject searchNodeJson = new JSONObject();
        searchNodeJson.put("id", networkId);
        searchNodeJson.put("score", score);
        searchNodeJson.put("description", description);
        searchNodeJson.put("mergeLevel", networkId.split("\\.").length - path.split("\\.").length);
        searchResultJson.add(searchNodeJson);
      }
    }
    return searchResultJson.toString();
  }
}
