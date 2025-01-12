package org.apache.zeppelin.iginx.util;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLMUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(LLMUtils.class);
  private static final String API_KEY = "204a3ea9bf39f18dd9bf32c71ecbb607.mITgz6pgV7Hzj27A";
  private static final String API_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions";
  private static final String MODEL = "GLM-4-Flash";

  /** 获取 LLM 的回答 */
  public static String getResponse(String prompt) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      // 构建请求体
      JSONObject requestBody = new JSONObject();
      requestBody.put("model", MODEL);
      requestBody.put(
          "messages",
          new JSONArray()
              .fluentAdd(new JSONObject().fluentPut("role", "user").fluentPut("content", prompt)));

      // 创建 HTTP POST 请求
      HttpPost postRequest = new HttpPost(API_URL);
      postRequest.setHeader("Content-Type", "application/json");
      postRequest.setHeader("Authorization", "Bearer " + API_KEY);
      postRequest.setEntity(new StringEntity(requestBody.toString(), StandardCharsets.UTF_8));

      // 发送请求并获取响应
      try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        if (response.getStatusLine().getStatusCode() != 200 || responseBody.isEmpty()) {
          return "Error: " + response.getStatusLine().getStatusCode() + " - " + responseBody;
        }

        JSONObject jsonResponse = JSONObject.parseObject(responseBody);
        JSONArray choices = jsonResponse.getJSONArray("choices");
        if (choices == null || choices.isEmpty()) {
          return "Error: Missing or empty 'choices' in response";
        }

        String message = choices.getJSONObject(0).getJSONObject("message").getString("content");
        LOGGER.info("get LLM response: {}", message);
        return message;
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred", e);
      return "Error occurred: " + e.getMessage();
    }
  }

  /** 聚合 merge */
  public static String getConcept(List<NetworkTreeNode> nodes) {
    StringBuilder conceptName = new StringBuilder();
    for (NetworkTreeNode node : nodes) {
      conceptName.append(node.getName()).append(";");
    }
    String prompt =
        "你是一个概括大师，我将给你几个用‘;’分隔的中文短语，请你将它们概括成一个中文短语。注意仅需返回概括结果并将结果用大括号包裹。\n需要概括的中文短语是: "
            + conceptName;
    String standardResponse = getStandardResponse(getResponse(prompt));
    if (standardResponse.isEmpty()) {
      standardResponse = getStandardResponse(getResponse(prompt));
    }
    LOGGER.info("get concept result: {}", standardResponse);
    return standardResponse;
  }

  /** 分析关系 relation */
  public static String getRelation(String name1, String name2) {
    String prompt =
        "你是一个概念大师，请你给出“" + name1 + "”和“" + name2 + "”之间切实具体、简洁精炼的关系，最好不超过10个字，并用大括号包裹返回，如\"{包含}\"。";
    String standardResponse = getStandardResponse(getResponse(prompt));
    if (standardResponse.isEmpty()) {
      standardResponse = getStandardResponse(getResponse(prompt));
    }
    LOGGER.info("get relation result: {}", standardResponse);
    return standardResponse;
  }

  private static String getStandardResponse(String str) {

    int start = str.indexOf('{');
    int end = str.lastIndexOf('}');
    String res = "";
    if (start != -1 && end != -1 && start < end) {
      res = str.substring(start + 1, end);
    }
    res = res.replaceAll("\"", "");
    return res;
  }
}
