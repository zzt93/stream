package cn.superid.collector.entity.view;

import com.google.gson.Gson;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 更详尽的页面浏览的统计信息
 * @author zzt
 */
public class RichPageStatistic implements Serializable {

  private static final Gson gson = new Gson();
  private Timestamp epoch;
  private long pv;
  private long uv;
  private long uvSigned;
  private Long affairId;
  private Long allianceId;
  private Long targetId;
  private String deviceType;

  public RichPageStatistic() {
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

  public Long getAffairId() {
    return affairId;
  }

  public void setAffairId(Long affairId) {
    this.affairId = affairId;
  }

  public Long getAllianceId() {
    return allianceId;
  }

  public void setAllianceId(Long allianceId) {
    this.allianceId = allianceId;
  }

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

  public String getDeviceType() {
    return deviceType;
  }

  public void setDeviceType(String deviceType) {
    this.deviceType = deviceType;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
