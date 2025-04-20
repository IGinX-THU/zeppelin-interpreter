package org.apache.zeppelin.iginx.interpreter;

import cn.edu.tsinghua.iginx.session.Session;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;

public abstract class AbstractExtensionInterpreter {
  public final IginxDao iginx;

  public AbstractExtensionInterpreter(Session session, String milvusHost, int milvusPort) {
    this.iginx = IginxDao.getInstance(session, milvusHost, milvusPort);
  }

  public abstract boolean canInterpret(String sql);

  public abstract InterpreterResult interpret(String statement, InterpreterContext context);
}
