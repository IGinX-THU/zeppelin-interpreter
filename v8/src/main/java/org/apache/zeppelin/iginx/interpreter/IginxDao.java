package org.apache.zeppelin.iginx.interpreter;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Column;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.ClusterNode;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.Relation;
import org.apache.zeppelin.iginx.interpreter.dataproperty.entry.SearchedNode;
import org.apache.zeppelin.iginx.interpreter.udfgenerator.GeneratedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IginxDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginxDao.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Session session;
  private final String milvusHost;
  private final int milvusPort;
  private static volatile IginxDao instance;

  public IginxDao(Session session, String milvusHost, int milvusPort) {
    this.session = session;
    this.milvusHost = milvusHost;
    this.milvusPort = milvusPort;
  }

  public static IginxDao getInstance(Session session, String milvusHost, int milvusPort) {
    if (instance == null) {
      synchronized (IginxDao.class) {
        if (instance == null) {
          instance = new IginxDao(session, milvusHost, milvusPort);
        }
      }
    }
    return instance;
  }

  public List<Column> getPathOf(String iginxPattern) {
    Preconditions.checkArgument(StringUtils.isNotBlank(iginxPattern));

    String sql = String.format("select path, type from (show columns %s);", iginxPattern);
    List<List<Object>> values = executeSql(sql);

    List<Column> columns = new ArrayList<>();
    for (List<Object> row : values) {
      String path = new String((byte[]) row.get(0), StandardCharsets.UTF_8);
      String typeStr = new String((byte[]) row.get(1), StandardCharsets.UTF_8);
      DataType type = DataType.valueOf(typeStr);
      columns.add(new Column(path, type));
    }

    return columns;
  }

  public Multimap<ClusterNode, String> getGroupingOf(
      String iginxPattern, String fetchFunction, String clusterFunction, int target) {
    Preconditions.checkArgument(StringUtils.isNotBlank(iginxPattern));
    Preconditions.checkArgument(StringUtils.isNotBlank(fetchFunction));
    Preconditions.checkArgument(StringUtils.isNotBlank(clusterFunction));

    String fetchSql =
        String.format(
            "select `%s(path)` as path, `%<s(description)` as description, `%<s(embedding)` as embedding"
                + " from (select %<s(*, pattern='%s',level=0) from (show columns ###))",
            fetchFunction, iginxPattern);

    String sql =
        String.format(
            "select `%s(path)`, `%<s(cluster)`" + " from (select %<s(*, target='%d') from (%s));",
            clusterFunction, target, fetchSql);

    List<List<Object>> values = executeSql(sql);
    Multimap<ClusterNode, String> groupingMap = HashMultimap.create();
    for (List<Object> row : values) {
      String path = new String((byte[]) row.get(0), StandardCharsets.UTF_8);
      String clusterKey = new String((byte[]) row.get(1), StandardCharsets.UTF_8);
      groupingMap.put(new ClusterNode(clusterKey, "<description example>: " + clusterKey), path);
    }

    return groupingMap;
  }

  public List<SearchedNode> search(
      String iginxPattern, String keywords, String topK, String function) {
    Preconditions.checkArgument(StringUtils.isNotBlank(iginxPattern));
    Preconditions.checkArgument(StringUtils.isNotBlank(topK));
    Preconditions.checkArgument(StringUtils.isNotBlank(keywords));
    Preconditions.checkArgument(StringUtils.isNotBlank(function));

    String sourceSql = String.format("select \"%s\" as description", keywords);
    String encodeSql =
        String.format(
            "select `%s(description)` as description, `%<s(embedding)` as embedding"
                + " from (select %<s(*) from (%s))",
            "default_encode", sourceSql);

    String sql =
        String.format(
            "select `%s(path)`,`%<s(score)`, `%<s(similarity)`"
                + " from (select %<s(*, pattern='%s', topk=%s) from (%s));",
            function, iginxPattern, topK, encodeSql);

    List<List<Object>> values = executeSql(sql);

    List<SearchedNode> pairs = new ArrayList<>();
    for (List<Object> row : values) {
      String pathSepDot = new String((byte[]) row.get(0), StandardCharsets.UTF_8);
      double score = (double) row.get(1);
      String descriptionStr = new String((byte[]) row.get(2), StandardCharsets.UTF_8);
      pairs.add(new SearchedNode(pathSepDot, descriptionStr, score));
    }
    return pairs;
  }

  public List<Relation> analyseMainRelation(
      List<String> sourcePaths, List<String> targetPaths, String function) {
    Preconditions.checkNotNull(sourcePaths);
    Preconditions.checkNotNull(targetPaths);
    Preconditions.checkArgument(StringUtils.isNotBlank(function));

    ArrayNode sourceArrayNode = MAPPER.createArrayNode();
    for (String sourcePath : sourcePaths) {
      sourceArrayNode.add(sourcePath);
    }
    String sourcePathsJson = sourceArrayNode.toString();

    ArrayNode targetArrayNode = MAPPER.createArrayNode();
    for (String targetPath : targetPaths) {
      targetArrayNode.add(targetPath);
    }
    String targetPathsJson = targetArrayNode.toString();

    String sql =
        String.format(
            "select `%s(source)`, `%<s(target)`, `%<s(score)`, `%<s(description)`"
                + " from (select %<s(*, sources='%s', targets='%s', host='%s', port='%d') from (show columns ###));",
            function, sourcePathsJson, targetPathsJson, milvusHost, milvusPort);

    List<List<Object>> values = executeSql(sql);

    List<Relation> relations = new ArrayList<>();
    for (List<Object> row : values) {
      String source = new String((byte[]) row.get(0), StandardCharsets.UTF_8);
      String target = new String((byte[]) row.get(1), StandardCharsets.UTF_8);
      double score = (double) row.get(2);
      String description = new String((byte[]) row.get(3), StandardCharsets.UTF_8);
      relations.add(new Relation(source, target, score, description));
    }
    return relations;
  }

  public GeneratedResult generateUdf(String description, String type, String function) {
    Preconditions.checkNotNull(description);
    Preconditions.checkNotNull(type);
    Preconditions.checkArgument(StringUtils.isNotBlank(function));

    String sql =
        String.format(
            "select `%s(prompt)`, `%<s(udf)` from (select %<s(*) from (select '%s' as type, '%s' as description));",
            function, type.replace("'", "\\'"), description.replace("'", "\\'"));
    List<List<Object>> values = executeSql(sql);
    return new GeneratedResult(
        new String((byte[]) values.get(0).get(0), StandardCharsets.UTF_8),
        new String((byte[]) values.get(0).get(1), StandardCharsets.UTF_8));
  }

  private List<List<Object>> executeSql(String sql) {
    SessionExecuteSqlResult sqlResult;
    try {
      sqlResult = session.executeSql(sql);
    } catch (SessionException e) {
      throw new RuntimeException("Failed to execute SQL: " + sql, e);
    }
    return sqlResult.getValues();
  }
}
