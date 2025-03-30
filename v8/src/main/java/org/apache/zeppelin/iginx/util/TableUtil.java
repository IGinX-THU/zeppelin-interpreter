package org.apache.zeppelin.iginx.util;

import cn.edu.tsinghua.iginx.session.Column;
import java.util.List;

public class TableUtil {

  public static String buildTableFromColumns(List<Column> columns) {
    StringBuilder builder = new StringBuilder();
    builder.append("path").append("\t").append("type").append("\n");
    for (Column column : columns) {
      builder
          .append(convertToHTMLString(column.getPath()))
          .append("\t")
          .append(column.getDataType())
          .append("\n");
    }
    return builder.toString();
  }

  public static String buildSingleFormResult(List<List<String>> queryList) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < queryList.size(); i++) {
      List<String> row = queryList.get(i);
      for (String val : row) {
        if (i != 0) {
          val = convertToHTMLString(val);
        }
        builder.append(val).append("\t");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append("\n");
    }
    return builder.toString();
  }

  private static String convertToHTMLString(String str) {
    return str.contains("\n")
        ? str.replace("\n", "<br>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;")
        : str;
  }
}
