package cn.superid.collector.entity;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author zzt
 */
public class PageStatistic implements Serializable {

  private Timestamp epoch;
  private long pv;
  private long uv;
  private long uvSigned;

  public Timestamp getEpoch() {
    return epoch;
  }

  public void setEpoch(Timestamp epoch) {
    this.epoch = epoch;
  }

  public long getUv() {
    return uv;
  }

  public void setUv(long uv) {
    this.uv = uv;
  }

  public long getUvSigned() {
    return uvSigned;
  }

  public void setUvSigned(long uvSigned) {
    this.uvSigned = uvSigned;
  }

  public long getPv() {
    return pv;
  }

  public void setPv(long pv) {
    this.pv = pv;
  }
}
