package org.apache.zeppelin.iginx.interpreter.DataSync;

import cn.edu.tsinghua.iginx.session.Session;
import java.util.Arrays;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.iginx.interpreter.AbstractExtensionInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSyncInterpreter extends AbstractExtensionInterpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSyncInterpreter.class);

  public DataSyncInterpreter(Session session, String milvusHost, int milvusPort) {
    super(session, milvusHost, milvusPort);
  }

  @Override
  public boolean canInterpret(String sql) {
    String cmd = sql.trim().split(" ")[0];
    switch (cmd) {
      case ">data.synchronization":
      case ">data.property.synchronization":
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
        case ">data.synchronization":
        case ">>data.property.synchronization":
          return displayDataSynchronization(args);
        default:
          throw new IllegalArgumentException("Invalid command: " + cmd);
      }
    } catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }

  private InterpreterResult displayDataSynchronization(String[] args) {
    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    String table;
    if (args.length == 3) {
      table = iginx.dataSynchronization(args[0], args[1], args[2]);
    } else {
      table = iginx.dataSynchronization("default_insert", "default_encode", "default_describe");
    }
    interpreterResult.add(InterpreterResult.Type.TABLE, table);
    return interpreterResult;
  }
}
