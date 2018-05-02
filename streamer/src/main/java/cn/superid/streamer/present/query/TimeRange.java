package cn.superid.streamer.present.query;

import java.sql.Timestamp;

/**
 * @author zzt
 */
public class TimeRange {

  private Timestamp from;
  private Timestamp to;

  public Timestamp getFrom() {
    return from;
  }

  public void setFrom(Timestamp from) {
    this.from = from;
  }

  public Timestamp getTo() {
    return to;
  }

  public void setTo(Timestamp to) {
    this.to = to;
  }
}
