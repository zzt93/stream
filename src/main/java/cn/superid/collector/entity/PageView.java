package cn.superid.collector.entity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.StringJoiner;

/**
 * @author zzt
 */
public class PageView {

  private Timestamp epoch;
  private String ip;

  private String userId;
  private String id;
  private String device;
  private String pageUri;

  private final HashMap<String, Object> extra = new HashMap<>();

  public PageView() {
  }

  /**
   * @param split The order of is same with {@link #toString()}
   */
  public PageView(String[] split) {
    epoch = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(split[0])));
    ip = split[1];
    userId = split[2];
    id = split[3];
    device = split[4];
    pageUri = split[5];
  }

  public Timestamp getEpoch() {
    return epoch;
  }

  public String getIp() {
    return ip;
  }

  public String getUserId() {
    return userId;
  }

  public String getId() {
    return id;
  }

  public String getDevice() {
    return device;
  }

  public String getPageUri() {
    return pageUri;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public void setPageUri(String pageUri) {
    this.pageUri = pageUri;
  }

  public void setEpoch(Timestamp epoch) {
    this.epoch = epoch;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" ");
    return joiner.add("" + epoch.getTime()).add(ip).add(userId).add(id).add(device).add(pageUri).toString();
  }
}
