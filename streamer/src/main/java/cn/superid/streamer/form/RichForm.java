package cn.superid.streamer.form;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.sql.Timestamp;

/**
 * 按照更多维度查询pv uv
 * @author dufeng
 * @create: 2018-10-15 10:48
 */
@ApiModel()
public class RichForm {
    /**
     * 事务id
     */
    @ApiModelProperty(value = "事务id", example = "12345")
    private long affairId;
    /**
     * 目标id
     */
    @ApiModelProperty(value = "目标id", example = "12345")
    private long targetId;
    /**
     * 时间范围起始点
     */
    @ApiModelProperty(value = "查询时间范围开始点", example = "2018-10-15T02:41:00.000Z")
    private Timestamp from;
    /**
     * 时间范围结束点
     */
    @ApiModelProperty(value = "查询时间范围结束点", example = "2018-10-25T02:41:00.000Z")
    private Timestamp to;
    /**
     * 时间单位：minute、hour、day
     */
    @ApiModelProperty(value = "查询的单位", example = "minute、hour、day")
    private String timeUnit;
    /**
     * 设备类型：Mac、Windows、iPhone、Android、其它
     */
    @ApiModelProperty(value = "用户设备", example = "Mac、Windows、iPhone、Android、其它")
    private String devType;

    public long getAffairId() {
        return affairId;
    }

    public void setAffairId(long affairId) {
        this.affairId = affairId;
    }

    public long getTargetId() {
        return targetId;
    }

    public void setTargetId(long targetId) {
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


}
