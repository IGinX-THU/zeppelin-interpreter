package org.apache.zeppelin.iginx.interpreter.dataproperty;

import cn.edu.tsinghua.iginx.session.Column;
import cn.edu.tsinghua.iginx.session.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.velocity.VelocityContext;
import org.apache.zeppelin.iginx.interpreter.AbstractExtensionInterpreter;
import org.apache.zeppelin.iginx.interpreter.dataproperty.network.NetworkService;
import org.apache.zeppelin.iginx.util.TableUtil;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPropertyInterpreter extends AbstractExtensionInterpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPropertyInterpreter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, NetworkService> networkMap = new HashMap<>();
  private static final String INTERNAL_STATEMENT_PREFIX = ">data.property";
  private static final String UDF = "UDF";

  public DataPropertyInterpreter(Session session, String milvusHost, int milvusPort) {
    super(session, milvusHost, milvusPort);
  }

  @Override
  public boolean canInterpret(String sql) {
    String cmd = sql.trim().split(" ")[0];
    switch (cmd) {
      case ">network":
      case ">network.grouping":
      case INTERNAL_STATEMENT_PREFIX + ".expand":
      case INTERNAL_STATEMENT_PREFIX + ".search":
      case INTERNAL_STATEMENT_PREFIX + ".clear":
        return true;
      default:
        return false;
    }
  }

  @Override
  public InterpreterResult interpret(String statement, InterpreterContext context) {
    // 截取第一个空格之前的内容和之后的内容
    String[] strings = statement.trim().split(" ");
    String cmd = strings[0];
    String[] args = Arrays.copyOfRange(strings, 1, strings.length);

    try {
      switch (cmd) {
        case ">network":
          return displayDataPropertyGraph(context, args, false);
        case ">network.grouping":
          return displayDataPropertyGraph(context, args, true);
        case INTERNAL_STATEMENT_PREFIX + ".expand":
          return expandDataPropertyGraph(args);
        case INTERNAL_STATEMENT_PREFIX + ".search":
          return searchDataProperty(args);
        case INTERNAL_STATEMENT_PREFIX + ".clear":
          return new InterpreterResult(
              InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, "");
        default:
          throw new IllegalArgumentException("Invalid command: " + cmd);
      }
    } catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }

  private InterpreterResult displayDataPropertyGraph(
      InterpreterContext context, String[] args, boolean needMerge) {
    String iginxPattern = String.join(" ", args);
    iginxPattern = StringUtils.isEmpty(iginxPattern) ? "*" : iginxPattern;
    List<Column> columns = iginx.getPathOf(iginxPattern);

    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);

    Pattern dotPattern = Pattern.compile("\\.");
    List<String[]> paths =
        columns.stream().map(Column::getPath).map(dotPattern::split).collect(Collectors.toList());
    NetworkService networkService =
        new NetworkService(needMerge, true, context.getParagraphId(), paths, iginx, iginxPattern);
    networkMap.put(context.getParagraphId(), networkService);

    VelocityContext velocityContext = new VelocityContext();
    velocityContext.put("paragraphId", context.getParagraphId());

    String html = networkService.initNetwork(velocityContext);
    interpreterResult.add(InterpreterResult.Type.HTML, html);
    String table = TableUtil.buildTableFromColumns(columns);
    interpreterResult.add(InterpreterResult.Type.TABLE, table);

    return interpreterResult;
  }

  private InterpreterResult expandDataPropertyGraph(String[] args) {
    Preconditions.checkArgument(args.length >= 2, "Invalid number of arguments: " + args.length);
    Preconditions.checkArgument(
        args[args.length - 1].startsWith(UDF),
        "Invalid ending of arguments: " + args[args.length - 1]);
    String paragraphId = args[0];
    String nodeId = String.join(" ", Arrays.copyOfRange(args, 1, args.length - 1));
    String function = args[args.length - 1].substring(UDF.length());

    NetworkService networkService = networkMap.get(paragraphId);
    Preconditions.checkNotNull(networkService, "Network service not found: " + paragraphId);

    String msg = networkService.handleNodeClick(nodeId, function);
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, msg);
  }

  private InterpreterResult searchDataProperty(String[] args) {
    Preconditions.checkArgument(
        args.length >= 4 && Integer.parseInt(args[1]) > 0 && args[2].startsWith(UDF),
        "Invalid arguments");
    String paragraphId = args[0];
    String topK = args[1];
    String function = args[2].substring(UDF.length());
    String keywords = String.join(" ", Arrays.copyOfRange(args, 3, args.length));

    NetworkService networkService = networkMap.get(paragraphId);
    Preconditions.checkNotNull(networkService, "Network service not found: " + paragraphId);

    String msg = networkService.handleSearch(keywords, topK, function);
    LOGGER.info("msg is {}", msg);
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, msg);
  }
}
