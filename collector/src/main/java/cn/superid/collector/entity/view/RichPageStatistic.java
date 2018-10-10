package cn.superid.collector.entity.view;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 更详尽的页面浏览的统计信息
 * @author zzt
 */
public class RichPageStatistic implements Serializable {

  private long id;
  private Timestamp epoch;
  private long pv;
  private long uv;
  private long uvSigned;
  private long affairId;
  private long allianceId;
  private long targetId;
  private boolean publicIp;
  private String deviceType;

  public RichPageStatistic() {
  }

  public RichPageStatistic(Timestamp epoch) {
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

  public long getAffairId() {
    return affairId;
  }

  public void setAffairId(long affairId) {
    this.affairId = affairId;
  }

  public long getAllianceId() {
    return allianceId;
  }

  public void setAllianceId(long allianceId) {
    this.allianceId = allianceId;
  }

  public long getTargetId() {
    return targetId;
  }

  public void setTargetId(long targetId) {
    this.targetId = targetId;
  }

  public boolean isPublicIp() {
    return publicIp;
  }

  public void setPublicIp(boolean publicIp) {
    this.publicIp = publicIp;
  }

  public String getDeviceType() {
    return deviceType;
  }

  public void setDeviceType(String deviceType) {
    this.deviceType = deviceType;
  }
}
