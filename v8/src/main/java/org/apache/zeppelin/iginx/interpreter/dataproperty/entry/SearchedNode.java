package org.apache.zeppelin.iginx.interpreter.dataproperty.entry;

import lombok.NonNull;
import lombok.Value;

@Value
public class SearchedNode {
  @NonNull String path;
  @NonNull String description;
  double score;
}
