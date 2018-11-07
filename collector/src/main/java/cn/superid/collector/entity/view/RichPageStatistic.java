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
  private String epoch;
  private long pv;
  private long uv;
  private long uvSigned;
  private Long affairId;
  private Long allianceId;
  private Long targetId;
  private String deviceType;

  public RichPageStatistic() {
  }

  public RichPageStatistic(String epoch, long pv, long uv, long uvSigned) {
    this.epoch = epoch;
    this.pv = pv;
    this.uv = uv;
    this.uvSigned = uvSigned;
  }

  public String getEpoch() {
    return epoch;
  }

  public void setEpoch(String epoch) {
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

  public RichPageStatistic setAffairId(Long affairId) {
    this.affairId = affairId;
    return this;
  }

  public Long getAllianceId() {
    return allianceId;
  }

  public RichPageStatistic setAllianceId(Long allianceId) {
    this.allianceId = allianceId;
    return this;
  }

  public Long getTargetId() {
    return targetId;
  }

  public RichPageStatistic setTargetId(Long targetId) {
    this.targetId = targetId;
    return this;
  }

  public String getDeviceType() {
    return deviceType;
  }

  public RichPageStatistic setDeviceType(String deviceType) {
    this.deviceType = deviceType;
    return this;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
