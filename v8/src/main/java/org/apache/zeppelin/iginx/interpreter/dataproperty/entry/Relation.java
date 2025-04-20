package org.apache.zeppelin.iginx.interpreter.dataproperty.entry;

import lombok.NonNull;
import lombok.Value;

@Value
public class Relation {
  @NonNull String fromPath;
  @NonNull String toPath;
  double score;
  @NonNull String description;
}
