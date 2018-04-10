package cn.superid.collector.entity;

import com.google.gson.Gson;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;

/**
 * @author zzt
 */
public class PageView {

  private static final Gson gson = new Gson();
  private final HashMap<String, Object> extra = new HashMap<>();
  private Timestamp epoch;
  private String ip;
  private String userId;
  private String id;
  private String device;
  private String pageUri;

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

  public static PageView fromString(String string) {
    return gson.fromJson(string, PageView.class);
  }

  public Timestamp getEpoch() {
    return epoch;
  }

  public void setEpoch(Timestamp epoch) {
    this.epoch = epoch;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getPageUri() {
    return pageUri;
  }

  public void setPageUri(String pageUri) {
    this.pageUri = pageUri;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
