package cn.superid.collector.entity;

import com.google.gson.Gson;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author zzt
 */
public class PageView implements Serializable {

  private static final Gson gson = new Gson();
  private String clientIp;
  private String device;
  private Timestamp epoch;
  private String frontVersion;
  private String id;
  private String pageUri;
  private String serverIp;
  private String userId;

  public PageView() {
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

  public String getClientIp() {
    return clientIp;
  }

  public void setClientIp(String clientIp) {
    this.clientIp = clientIp;
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

  public String getServerIp() {
    return serverIp;
  }

  public void setServerIp(String serverIp) {
    this.serverIp = serverIp;
  }

  public String getFrontVersion() {
    return frontVersion;
  }

  public void setFrontVersion(String frontVersion) {
    this.frontVersion = frontVersion;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }

  public static class PageBuilder {

    private String id;
    private String pageUri;
    private String userId;
    private Timestamp epoch;
    private String dev;
    private String serverIp;
    private String clientIp;

    public PageView build() {
      PageView pageView = new PageView();
      pageView.setId(id);
      pageView.setId(pageUri);
      pageView.setPageUri(userId);
      pageView.setEpoch(epoch);
      pageView.setDevice(dev);
      pageView.setClientIp(clientIp);
      pageView.setServerIp(serverIp);
      return pageView;
    }

    public PageBuilder setId(String id) {
      this.id = id;
      return this;
    }

    public PageBuilder setPageUri(String pageUri) {
      this.pageUri = pageUri;
      return this;
    }

    public PageBuilder setUserId(String userId) {
      this.userId = userId;
      return this;
    }

    public PageBuilder setTimestamp(Timestamp epoch) {
      this.epoch = epoch;
      return this;
    }

    public PageBuilder setDevice(String device) {
      this.dev = device;
      return this;
    }

    public PageBuilder setServerIp(String serverIp) {
      this.serverIp = serverIp;
      return this;
    }

    public PageBuilder setClientIp(String clientIp) {
      this.clientIp = clientIp;
      return this;
    }
  }
}
