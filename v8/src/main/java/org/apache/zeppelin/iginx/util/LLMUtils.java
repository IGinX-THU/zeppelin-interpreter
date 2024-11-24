package org.apache.zeppelin.iginx.util;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
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
  private static final Logger logger = LoggerFactory.getLogger(LLMUtils.class);
  private static final String API_KEY = "204a3ea9bf39f18dd9bf32c71ecbb607.mITgz6pgV7Hzj27A";
  private static final String API_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions";

  /**
   * 输入 prompt ，LLM 返回响应结果
   * 目前使用的是智谱 GLM-4 模型
   *
   * @param prompt
   * @return
   */
  public static String getResponse(String prompt) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      // 构建请求体
      JSONObject requestBody = new JSONObject();
      requestBody.put("model", "glm-4"); // 添加模型参数

      // 构建消息数组
      JSONArray messages = new JSONArray();
      JSONObject userMessage = new JSONObject();
      userMessage.put("role", "user");
      userMessage.put("content", prompt);
      messages.add(userMessage);
      requestBody.put("messages", messages);

      // 创建 HTTP POST 请求
      HttpPost postRequest = new HttpPost(API_URL);
      postRequest.setHeader("Content-Type", "application/json");
      postRequest.setHeader("Authorization", "Bearer " + API_KEY);

      // 设置请求体
      StringEntity entity = new StringEntity(requestBody.toString(), "UTF-8");
      postRequest.setEntity(entity);

      // 发送请求并获取响应
      try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
        int statusCode = response.getStatusLine().getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");

        if (statusCode == 200) {
          if (responseBody == null || responseBody.isEmpty()) {
            return "Error: Empty response body";
          }
          JSONObject jsonResponse = JSONObject.parseObject(responseBody);
          if (!jsonResponse.containsKey("choices")) {
            return "Error: Missing 'choices' in response";
          }
          JSONArray choices = jsonResponse.getJSONArray("choices");
          if (choices.isEmpty()) {
            return "Error: Empty 'choices' in response";
          }
          String message = choices.getJSONObject(0).getJSONObject("message").getString("content");
          logger.info("get LLM response: {}", message);
          return message;
        } else {
          return "Error: " + statusCode + " - " + responseBody;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.info("get LLM response error");
      return "Error occurred: " + e.getMessage();
    }
  }

  /**
   * 获取概括的单词，由于有些情况确实不适合合并，允许返回 wordA-wordB 的样式
   * 用于 【顶层合并】
   *
   * @param nodes
   * @return
   */
  public static String getConcept(List<TreeNode> nodes) {
    StringBuilder conceptName = new StringBuilder();
    for (TreeNode node : nodes) {
      conceptName.append(node.getValue()).append(";");
    }
   //你是一个概括大师，我将给你多个短语，中间用';'来分隔，请给概括这些短语，返回一个短语。注意只需返回这个短语即可，短语使用大括号进行包裹。需要你合并概括的多和短语是：" + conceptName
    String res =
        getResponse(
            "You are a summarization master. I will provide multiple words or phases separated by ';'. Please summarize them into a single word or a phase if possible. If merging is not feasible, return a hyphen-separated string like 'apple-desk'."
                + "Return only the result wrapped in curly braces. Words: "
                + conceptName);
    res = getStandardResponse(res);
    if (res == null) {
      logger.info("getConcept: the form of LLM response is wrong");
      return getConcept(nodes);
    }
    return res;
  }

  public static String getRelation(String name, String str) {
    //        String res =
    // getResponse("你是一个概念大师，我将给你一个待匹配短语和若干个目标短语，提供的若干个目标短语中间用';'来分隔，请分析在概念上和待匹配短语具有非常非常强烈关联的目标短语，如有则返回该目标短语。" +
    //                "注意只需返回这个短语即可，短语使用大括号进行包裹，如果没有一个目标短语符合要求则直接返回一个大括号。\n待匹配短语为：" + name +
    // "\n若干目标短语为：" + str);
    String res =
        getResponse(
            "You are a concept master. I will provide you with a phrase to match and several target phrases, separated by ';'. Please analyze and identify the target phrase that is conceptually very, very strongly related to the phrase to match. If such a phrase exists, return that target phrase."
                + "Note that you should only return the phrase, wrapped in curly braces. If no target phrase meets the requirement, return an empty pair of curly braces.\nPhrase to match: "
                + name
                + "\nTarget phrases: "
                + str);

    res = getStandardResponse(res);
    return res;
  }

  /**
   * 将返回要求包裹的的 {} 除去，获取真正有用的内容
   *
   * @param text
   * @return
   */
  private static String getStandardResponse(String text) {

    int start = text.indexOf('{');
    int end = text.lastIndexOf('}');
    String res = null;
    if (start != -1 && end != -1 && start < end) {
      res = text.substring(start + 1, end);
      res = res.replaceAll("\"", "");
      return res;
    }
    return null;
  }
}
