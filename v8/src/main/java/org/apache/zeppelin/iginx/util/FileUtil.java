package org.apache.zeppelin.iginx.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import org.apache.zeppelin.iginx.IginxInterpreter8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginxInterpreter8.class);

  public static void writeFile(String content, String filePath) {
    try (FileOutputStream fos = new FileOutputStream(filePath);
        OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
        BufferedWriter writer = new BufferedWriter(osw)) { // 使用BufferedWriter提高效率

      writer.write(content);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String readFile(String filePath) {
    StringBuilder content = new StringBuilder();
    try (InputStream inputStream =
            IginxInterpreter8.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }
    } catch (IOException e) {
      LOGGER.error("load html error", e);
    }
    return content.toString();
  }

  public static String renderingHtml(String htmlPath, String... parameters) {
    String html = readFile(htmlPath);
    for (int i = 0; i < parameters.length; i = i + 2) {
      html = html.replace(parameters[i], parameters[i + 1]);
    }
    return html;
  }
}
