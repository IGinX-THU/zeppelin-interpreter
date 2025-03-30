package org.apache.zeppelin.iginx.interpreter.udfgenerator;

import cn.edu.tsinghua.iginx.session.Session;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.velocity.VelocityContext;
import org.apache.zeppelin.iginx.interpreter.AbstractExtensionInterpreter;
import org.apache.zeppelin.iginx.util.VelocityUtil;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfGeneratorInterpreter extends AbstractExtensionInterpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(UdfGeneratorInterpreter.class);
  private static final String DEFAULT_GENERATE_UDF_FUNCTION = "default_generate";

  private static final Map<String, String> CMD_MAP =
      new HashMap<String, String>() {
        {
          put(">generate.udf.cluster", "Cluster");
          put(">generate.udf.describe", "Describe");
          put(">generate.udf.encode", "Encode");
          put(">generate.udf.fetch", "Fetch");
          put(">generate.udf.generate", "Generate");
          put(">generate.udf.insert", "Insert");
          put(">generate.udf.relate", "Relate");
          put(">generate.udf.search", "Search");
        }
      };

  public UdfGeneratorInterpreter(Session session, String milvusHost, int milvusPort) {
    super(session, milvusHost, milvusPort);
  }

  @Override
  public boolean canInterpret(String sql) {
    String cmd = sql.trim().split(" ")[0];
    return CMD_MAP.containsKey(cmd);
  }

  @Override
  public InterpreterResult interpret(String statement, InterpreterContext context) {
    // 截取第一个空格之前的内容和之后的内容
    String[] strings = statement.trim().split(" ");
    String cmd = strings[0];
    String[] args = Arrays.copyOfRange(strings, 1, strings.length);

    try {
      String udfType = CMD_MAP.get(cmd);
      if (udfType == null) {
        throw new IllegalArgumentException("Invalid command: " + cmd);
      }
      return displayUdfGenerator(args, udfType);
    } catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }

  private InterpreterResult displayUdfGenerator(String[] args, String type) {
    try {
      String description = String.join(" ", args);
      GeneratedResult result = iginx.generateUdf(description, type, DEFAULT_GENERATE_UDF_FUNCTION);
      String prompt = result.getPrompt();
      String udf = result.getCode().replace("```python", "").replace("```", "").trim();
      VelocityContext velocityContext = new VelocityContext();
      velocityContext.put("prompt", prompt);
      velocityContext.put("python_text", udf);
      String html = VelocityUtil.generate("templates/udf-generator.vm", velocityContext);

      return new InterpreterResult(
          InterpreterResult.Code.SUCCESS, InterpreterResult.Type.HTML, html);
    } catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }
}
