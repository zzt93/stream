package cn.superid.collector.entity.option;

import cn.superid.collector.entity.AbstractEntity;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.google.gson.Gson;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * 移动端上报用户操作信息
 * 因为移动端为了减小用户流量消耗，可能会批量上传，把公共部分放在外面，差异部分放在列表中，所以数据结构和web端的不一样
 * @author dufeng
 * @create: 2018-07-18 16:25
 */
@ApiModel()
public class MobileOption extends AbstractEntity implements Serializable {
    private static final Gson gson = new Gson();

    /**
     * 硬件唯一标识
     */
    @JsonAlias("id")
    @ApiModelProperty(value = "硬件唯一标识" , example = "21f5fb45-9f72-592f-8bcf-def1167b1f56")
    private String viewId;

    /**
     * 用户id
     */
    @ApiModelProperty(value = "用户id" ,example = "546211")
    private long userId;



    /**
     * 设备类型(web、android、ios)
     */
    @ApiModelProperty(value = "客户端设备类型：web/android/ios" ,example = "web")
    private String devType;

    /**
     * app版本信息
     */
    @ApiModelProperty(value = "app版本，web上报前端工程版本" , example = "4.3.0")
    private String appVer;

    /**
     * 用户操作记录集合
     */
    @ApiModelProperty(value = "用户操作记录批次")
    private List<OptionEntry> innerEntries;

    /**
     * 服务端收到客户端上报请求的时间
     */
    @ApiModelProperty(hidden = true)
    private Timestamp epoch;

    /**
     * 服务端接收到上报数据的时间
     */
    @ApiModelProperty(hidden = true)
    private String uploadTime;

    /**
     * fasterxml.jackson需要用到空构造器
     */
    public MobileOption() {

    }


    @Override
    public String toString() {
        return gson.toJson(this);
    }

    /**
     * 对上报数据合法性进行校验
     *
     * @return
     */
    @Override
    public boolean validate() {


        return false;
    }

    public static Gson getGson() {
        return gson;
    }

    public String getViewId() {
        return viewId;
    }

    public void setViewId(String viewId) {
        this.viewId = viewId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getDevType() {
        return devType;
    }

    public void setDevType(String devType) {
        this.devType = devType;
    }

    public String getAppVer() {
        return appVer;
    }

    public void setAppVer(String appVer) {
        this.appVer = appVer;
    }

    public List<OptionEntry> getInnerEntries() {
        return innerEntries;
    }

    public void setInnerEntries(List<OptionEntry> innerEntries) {
        this.innerEntries = innerEntries;
    }

    public Timestamp getEpoch() {
        return epoch;
    }

    public void setEpoch(Timestamp epoch) {
        this.epoch = epoch;
    }

    public String getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(String uploadTime) {
        this.uploadTime = uploadTime;
    }

    /**
     * 存放变动部分的记录
     */
    @ApiModel()
    public static class OptionEntry {

        /**
         * 客户端ip地址
         */
        @ApiModelProperty(value ="用户操作时客户端ip地址" , example = "116.6.2.1")
        private String clientIp;
        /**
         * 行业线
         */
        @ApiModelProperty(value = "行业线",example = "0")
        private int businessLine;

        /**
         * 页面信息(安卓和IOS可能不是网址的一部分)
         */
        @ApiModelProperty(value = "页面标识",example = "/index/register.html")
        private String pageUri;

        /**
         * 页面元素(element)id
         */
        @ApiModelProperty(value = "页面元素",example = "register-button")
        private String eleId;

        /**
         * 用户操作事件附带的属性，比如点击一个提交按钮可以把表单中的内容上报
         */
        @ApiModelProperty(value = "页面元素附带属性",dataType = "java.util.Map[String,Object]",example = "{'key':'value'}")
        private Map<String, Object> attrs;

        /**
         * 客户操作时间，10或13位时间戳
         */
        @ApiModelProperty(value ="用户操作的时间，时间戳格式" , example = "1532413668")
        private String opTime;

        public int getBusinessLine() {
            return businessLine;
        }

        public void setBusinessLine(int businessLine) {
            this.businessLine = businessLine;
        }

        public String getPageUri() {
            return pageUri;
        }

        public void setPageUri(String pageUri) {
            this.pageUri = pageUri;
        }

        public String getEleId() {
            return eleId;
        }

        public void setEleId(String eleId) {
            this.eleId = eleId;
        }

        public Map<String, Object> getAttrs() {
            return attrs;
        }

        public void setAttrs(Map<String, Object> attrs) {
            this.attrs = attrs;
        }

        public String getOpTime() {
            return opTime;
        }

        public void setOpTime(String opTime) {
            this.opTime = opTime;
        }

        public String getClientIp() {
            return clientIp;
        }

        public void setClientIp(String clientIp) {
            this.clientIp = clientIp;
        }
    }
}


