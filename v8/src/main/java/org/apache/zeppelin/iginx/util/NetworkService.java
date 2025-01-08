package org.apache.zeppelin.iginx.util;

import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkService {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkService.class);
  private static final Double RELATION_THRESHOLD = 0.8; // 关系阈值
  private static final Integer RELATION_DEPTH_LEVEL = 3; // 关系深度层级
  private static final Integer MERGE_DEPTH_LEVEL = 3; // 聚合时考虑的结点层级
  private static final Integer MERGE_MIN_SIZE = 5; // 需要聚类的最小值
  private static final Integer EMBEDDING_DEPTH_LEVEL = 3; // 计算embedding时考虑的结点层级
  private static final Double EMBEDDING_WEIGHT = 0.8; // 计算embedding时自身的权重
  private static final String MERGE_SQL_STR =
      "select merge(*, str='@@@', x=" + EMBEDDING_WEIGHT + ") from (show columns ###);";
  private Boolean needMerge; // 是否需要合并
  private Boolean needRelation; // 是否需要计算关系
  private Session session;
  private String paragraphId;
  private List<List<String>> columnPath;
  private NetworkTreeNode root;
  private Map<String, NetworkTreeNode> allNodeMap = new HashMap<>();
  private Map<String, Map<String, Relation>> relationMap = new HashMap<>();

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
  }

  public String initNetwork() {
    LOGGER.info("{} {} {}", needMerge, needRelation, paragraphId);
    root = new NetworkTreeNode("rootId", "数据资产", null, 0);
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
    String jsonString = JSON.toJSONString(nodeList);
    LOGGER.info("the jsonString is {}", jsonString);
    return loadHtmlTemplate().replace("NODE_LIST", jsonString);
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
      collapseNode(node, removeMap);
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
                  currentNode.getId(), // 父节点ID
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

  // todo:数据量很大时，updateNodes会几乎遍历所有结点，比较耗时，后续考虑借鉴懒标记思想优化？
  private void mergeForest(NetworkTreeNode root) {
    long startTime = System.currentTimeMillis();
    List<Map<String, Object>> result = new ArrayList<>();
    for (NetworkTreeNode childNode : root.getChildren().values()) {
      result.add(buildNodeJson(childNode));
    }
    if (result.size() < MERGE_MIN_SIZE) {
      LOGGER.info("the size of the forest is too small");
      return;
    }
    String str = "";
    try {
      str = JSON.toJSONString(result);
    } catch (Exception e) {
      LOGGER.error("JSON fail", e);
    }
    List<List<String>> queryList = null;
    try {
      String sql = MERGE_SQL_STR.replace("@@@", str);
      SessionExecuteSqlResult sqlResult = session.executeSql(sql);
      queryList = sqlResult.getResultInList(false, FormatUtils.DEFAULT_TIME_FORMAT, "");
    } catch (Exception e) {
      LOGGER.info("encounter error when executing sql statement:\n" + e.getMessage());
    }
    if (queryList == null || queryList.size() <= 1) {
      LOGGER.info("Invalid queryList or insufficient data");
      return;
    }
    LOGGER.info("the size of queryList is {}", queryList.size());

    // 根据label区分
    Map<String, List<NetworkTreeNode>> labelToNodesMap = new HashMap<>();
    for (int i = 1; i < queryList.size(); i++) {
      List<String> row = queryList.get(i);
      String label = row.get(0);
      String nodeName = row.get(1);
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
        NetworkTreeNode mergedNode =
            new NetworkTreeNode("rootId." + mergedName, mergedName, "rootId", 1);
        for (NetworkTreeNode node : nodesToMerge) {
          mergedNode.getChildren().put(node.getName(), node);
          updateNodes(node, mergedNode.getId(), 2);
          root.getChildren().remove(node.getName());
        }
        root.getChildren().put(mergedNode.getName(), mergedNode);
      }
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("mergeForest run time：" + (endTime - startTime) + "ms");
  }

  private Map<String, Object> buildNodeJson(NetworkTreeNode node) {
    Map<String, Object> nodeMap = new HashMap<>();
    nodeMap.put("name", node.getName());
    if (node.getDepth() < MERGE_DEPTH_LEVEL) {
      List<Map<String, Object>> childrenList = new ArrayList<>();
      for (NetworkTreeNode child : node.getChildren().values()) {
        childrenList.add(buildNodeJson(child));
      }
      nodeMap.put("children", childrenList);
    } else {
      nodeMap.put("children", new ArrayList<>());
    }
    return nodeMap;
  }

  private void updateNodes(NetworkTreeNode node, String parentId, int depth) {
    node.setParent(parentId);
    node.setDepth(depth);
    node.setId(parentId + "." + node.getName());
    for (NetworkTreeNode childNode : node.getChildren().values()) {
      updateNodes(childNode, node.getId(), depth + 1);
    }
  }

  private String loadHtmlTemplate() {
    String htmlTemplate = "static/vis/new_network.html";
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

    for (NetworkTreeNode child : node.getChildren().values()) {
      child.setShown(true);
      JSONObject nodeData = new JSONObject();
      nodeData.put("id", child.getId());
      nodeData.put("name", child.getName());
      nodeData.put("depth", child.getDepth());
      nodes.add(nodeData);

      JSONObject edgeData = new JSONObject();
      edgeData.put("from", node.getId());
      edgeData.put("to", child.getId());
      edges.add(edgeData);
    }

    addMap.put("nodes", nodes);
    addMap.put("edges", edges);
    addMap.put("links", links);
  }

  private void collapseNode(NetworkTreeNode node, JSONObject removeMap) {
    JSONArray nodes = new JSONArray();
    for (NetworkTreeNode child : node.getChildren().values()) {
      collectShownNodes(child, nodes);
    }
    removeMap.put("nodes", nodes);
  }

  private void collectShownNodes(NetworkTreeNode node, JSONArray nodes) {
    if (node.getShown()) {
      JSONObject nodeData = new JSONObject();
      nodeData.put("id", node.getId());
      nodes.add(nodeData);
      node.setShown(false);
      for (NetworkTreeNode child : node.getChildren().values()) {
        collectShownNodes(child, nodes); // 递归处理
      }
    }
  }
}
