package cn.superid.streamer.form;

import cn.superid.collector.util.DevUtil;
import cn.superid.streamer.compute.Unit;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.sql.Timestamp;

/**
 * 按照更多维度查询pv uv
 *
 * @author dufeng
 * @create: 2018-10-15 10:48
 */
@ApiModel()
public class RichForm {

  /**
   * 事务id
   */
  @ApiModelProperty(notes = "事务id", example = "12345")
  private Long affairId;
  /**
   * 目标id
   */
  @ApiModelProperty(notes = "目标id", example = "12345")
  private Long targetId;
  /**
   * 时间范围起始点
   */
  @ApiModelProperty(notes = "查询时间范围开始点", example = "2018-10-15T02:41:00.000Z")
  private Timestamp from;
  /**
   * 时间范围结束点
   */
  @ApiModelProperty(notes = "查询时间范围结束点", example = "2018-10-25T02:41:00.000Z")
  private Timestamp to;
  /**
   * 时间单位：minute/hour/day
   */
  @ApiModelProperty(notes = "查询的单位", example = "minute/hour/day")
  private String timeUnit;
  /**
   * 设备类型：Mac/Windows/iPhone/Android/
   */
  @ApiModelProperty(notes = "用户设备", example = "Mac/Windows/iPhone/Android/" + DevUtil.UNKNOWN)
  private String devType;

  public void validate() {
    Unit.valueOf(timeUnit.toUpperCase());

    if (from.after(to)) {
      throw new RuntimeException("开始时间大于结束时间！");
    }

    if (devType != null && !("Mac".equals(devType) || "Windows".equals(devType) || "iPhone".equals(devType)
        || "Android".equals(devType) || DevUtil.UNKNOWN.equals(devType))) {
      throw new RuntimeException("未知设备类型：" + devType);
    }
  }

  public Long getAffairId() {
    return affairId;
  }

  public void setAffairId(Long affairId) {
    this.affairId = affairId;
  }

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

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

  public String getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(String timeUnit) {
    this.timeUnit = timeUnit;
  }

  public String getDevType() {
    return devType;
  }

  public void setDevType(String devType) {
    this.devType = devType;
  }

  @Override
  public String toString() {
    return "RichForm{" +
        "affairId=" + affairId +
        ", targetId=" + targetId +
        ", from=" + from +
        ", to=" + to +
        ", timeUnit='" + timeUnit + '\'' +
        ", devType='" + devType + '\'' +
        '}';
  }
}
