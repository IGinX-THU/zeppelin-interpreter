package org.apache.zeppelin.iginx.util;

import com.alibaba.fastjson2.annotation.JSONField;

public class Relation {
  @JSONField(serialize = true)
  private String from;

  @JSONField(serialize = true)
  private String to;

  private Double score;

  @JSONField(serialize = true)
  private String relation;

  public Relation(String from, String to, Double score) {
    this.from = from;
    this.to = to;
    this.score = score;
    this.relation = "";
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String getTo() {
    return to;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public Double getScore() {
    return score;
  }

  public void setScore(Double score) {
    this.score = score;
  }

  public String getRelation() {
    return relation;
  }

  public void setRelation(String relation) {
    this.relation = relation;
  }
}
