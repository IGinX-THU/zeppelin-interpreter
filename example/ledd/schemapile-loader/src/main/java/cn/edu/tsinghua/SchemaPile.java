package cn.edu.tsinghua;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.Data;

public class SchemaPile {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Data
  public static class Schema {
    @JsonProperty("INFO")
    private Info info;

    @JsonProperty("TABLES")
    private Map<String, Table> tables;
  }

  @Data
  public static class Info {
    @JsonProperty("URL")
    private String url;
  }

  @Data
  public static class Table {
    @JsonProperty("COLUMNS")
    private Map<String, Column> columns;
  }

  @Data
  public static class Column {
    @JsonProperty("TYPE")
    private String type;

    @JsonProperty("VALUES")
    private List<Object> values;
  }

  private final Map<String, Schema> schemas;

  public SchemaPile(String json) throws JsonProcessingException {
    this.schemas = MAPPER.readValue(json, new TypeReference<Map<String, Schema>>() {});
  }

  public List<IginxColumn> toIginxColumn() {
    Set<String> paths = new HashSet<>();
    List<IginxColumn> result = new ArrayList<>();
    for (Schema schema : schemas.values()) {
      String url = schema.info.url;
      String prefix = parseUrl(url);
      for (Map.Entry<String, Table> tableEntry : schema.tables.entrySet()) {
        String tableName = tableEntry.getKey();
        Table table = tableEntry.getValue();
        for (Map.Entry<String, Column> entry : table.columns.entrySet()) {
          String columnName = entry.getKey();
          Column column = entry.getValue();
          List<Object> values = column.values;
          if (values == null || values.isEmpty()) {
            continue;
          }
          String pathName = escapeSource(prefix) + "." + escapeTableOrColumn(tableName) + "." + escapeTableOrColumn(columnName);
          if (!paths.add(pathName)) {
            continue;
          }

          try {
            DataType dataType = getDataType(column.type);
            Object[] columnData = castData(dataType, values);
            result.add(new IginxColumn(pathName, dataType, columnData));
          } catch (NumberFormatException e) {
            DataType dataType = DataType.BINARY;
            Object[] columnData = castData(dataType, values);
            result.add(new IginxColumn(pathName, dataType, columnData));
          }
        }
      }
    }
    return result;
  }

  private String escapeSource(String path) {
    path = path.replaceAll("[^0-9a-zA-Z.]", "_");
    // 替换 time 为 time_ 不区分大小写
    path = path.replaceAll("(?i)time", "time_");
    return path;
  }

  private String escapeTableOrColumn(String path) {
    path = path.replaceAll("[^0-9a-zA-Z]", "_");
    // 替换 time 为 time_ 不区分大小写
    path = path.replaceAll("(?i)time", "time_");
    return path;
  }

  private static Object[] castData(DataType dataType, List<Object> values)
      throws NumberFormatException {
    Object[] result = new Object[values.size()];
    for (int i = 0; i < values.size(); i++) {
      Object value = values.get(i);
      if (value == null) {
        result[i] = null;
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          if (value instanceof Boolean) {
            result[i] = value;
          } else {
            result[i] = Boolean.parseBoolean(value.toString());
          }
          break;
        case INTEGER:
          if (value instanceof Number) {
            result[i] = ((Number) value).intValue();
          } else {
            try {
              result[i] = Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
              result[i] = (int) Double.parseDouble(value.toString());
            }
          }
          break;
        case LONG:
          if (value instanceof Number) {
            result[i] = ((Number) value).longValue();
          } else {
            try {
              result[i] = Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
              result[i] = (long) Double.parseDouble(value.toString());
            }
          }
          break;
        case FLOAT:
          if (value instanceof Number) {
            result[i] = ((Number) value).floatValue();
          } else {
            result[i] = Float.parseFloat(value.toString());
          }
          break;
        case DOUBLE:
          if (value instanceof Number) {
            result[i] = ((Number) value).doubleValue();
          } else {
            result[i] = Double.parseDouble(value.toString());
          }
          break;
        case BINARY:
          result[i] = value.toString().getBytes(StandardCharsets.UTF_8);
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + dataType);
      }
    }
    return result;
  }

  private static DataType getDataType(String type) {
    switch (type) {
      case "bool":
      case "Boolean":
      case "bool(default: true)":
        return DataType.BOOLEAN;
      case "int2":
      case "INT2":
      case "int4":
      case "INT4":
      case "UnsignedTinyInt":
      case "UnsignedSmallInt":
      case "UnsignedInt":
      case "TinyInt":
      case "SmallInt":
      case "Int":
      case "Integer":
      case "PortNumber":
      case "VlanNumber":
        return DataType.INTEGER;
      case "INT8":
      case "UnsignedBigInt":
      case "BigInt":
      case "LONGINTEGER":
        return DataType.LONG;
      case "Float":
        return DataType.FLOAT;
      case "Float8":
      case "Double":
      case "Numeric":
      case "Number":
      case "Decimal":
      case "Real":
      case "DoublePrecision":
        return DataType.DOUBLE;
      default:
        return DataType.BINARY;
    }
  }

  private static String parseUrl(String url) {
    if (url.startsWith("https://")) {
      url = url.substring("https://".length());
    }

    String[] parts = url.split("/");
    int blobIndex = Arrays.asList(parts).indexOf("blob");
    String[] pathParts =
        (blobIndex != -1)
            ? Arrays.copyOfRange(parts, 1, blobIndex)
            : Arrays.copyOfRange(parts, 1, parts.length);

    return String.join("_", pathParts);
  }
}
