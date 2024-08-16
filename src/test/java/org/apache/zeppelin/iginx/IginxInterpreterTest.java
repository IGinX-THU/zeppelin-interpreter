package org.apache.zeppelin.iginx;

import java.util.Properties;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IginxInterpreterTest {

  private final IginxInterpreter interpreter = new IginxInterpreter(new Properties());
  private final InterpreterContext context =
      new InterpreterContext(
          "noteId",
          "paragraphId",
          "replName",
          "paragraphTitle",
          "text",
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

  @BeforeEach
  public void setUp() throws InterpreterException {
    interpreter.open();
  }

  @Test
  public void testShowClusterInfo() throws InterpreterException {
    InterpreterResult result = interpreter.interpret("show cluster info;", context);
    System.out.println(result);
  }
}
