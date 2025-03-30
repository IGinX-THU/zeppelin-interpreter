package org.apache.zeppelin.iginx.interpreter.udfgenerator;

import lombok.Value;

@Value
public class GeneratedResult {
  String prompt;
  String code;
}
