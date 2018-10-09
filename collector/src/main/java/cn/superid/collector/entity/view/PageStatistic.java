package cn.superid.collector.entity.view;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 页面浏览的统计信息
 * @author zzt
 */
public class PageStatistic implements Serializable {

  private long id;
  private Timestamp epoch;
  private long pv;
  private long uv;
  private long uvSigned;

  public PageStatistic() {
  }

  public PageStatistic(Timestamp epoch) {
    this.epoch = epoch;
    setId(epoch.getTime());
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

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
