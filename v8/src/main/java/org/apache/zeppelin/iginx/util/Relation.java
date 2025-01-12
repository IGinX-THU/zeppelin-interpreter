package org.apache.zeppelin.iginx.util;

public class Relation {
  private String from;
  private String to;
  private Double score;
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

  @Override
  public String toString() {
    return "Relation{"
        + "from='"
        + from
        + '\''
        + ", to='"
        + to
        + '\''
        + ", score="
        + score
        + ", relation='"
        + relation
        + '\''
        + '}';
  }
}
