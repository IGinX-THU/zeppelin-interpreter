package org.apache.zeppelin.iginx.interpreter.dataproperty.entry;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

@Value
public class ClusterNode {
  @NonNull String path;
  @EqualsAndHashCode.Exclude @NonNull String description;
}
