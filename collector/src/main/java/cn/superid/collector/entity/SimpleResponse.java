package cn.superid.collector.entity;

/**
 * @author zzt
 */
public class SimpleResponse {

  private final int code;

  public SimpleResponse(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
