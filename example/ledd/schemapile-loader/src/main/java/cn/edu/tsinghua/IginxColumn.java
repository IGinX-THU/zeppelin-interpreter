package cn.edu.tsinghua;

import cn.edu.tsinghua.iginx.thrift.DataType;
import lombok.Value;

@Value
public class IginxColumn {
  private String path;
  private DataType type;
  private Object[] values;
}
