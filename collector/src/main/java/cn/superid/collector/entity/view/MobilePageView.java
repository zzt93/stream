package cn.superid.collector.entity.view;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.google.gson.Gson;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

/**
 * 移动端上报用户浏览信息
 *
 * @author dufeng
 */
@ApiModel()
public class MobilePageView implements Serializable {

    private static final Gson gson = new Gson();

    @ApiModelProperty(hidden = true)
    private String device;
    /**
     * 服务端收到客户端上报请求的时间
     */
    @ApiModelProperty(hidden = true)
    private Timestamp epoch;
    @ApiModelProperty(hidden = true)
    private String frontVersion;
    /**
     * 硬件唯一标识id
     */
    @JsonAlias("id")
    @ApiModelProperty(value = "硬件唯一标识", example = "21f5fb45-9f72-592f-8bcf-def1167b1f56")
    private String viewId;
    @ApiModelProperty(hidden = true)
    private String serverIp;
    @ApiModelProperty(value = "用户id", example = "54321")
    private long userId;
    @ApiModelProperty(hidden = true)
    private String domain;

    /**
     * 客户端类型(web、android、ios)
     */
    @ApiModelProperty(value = "客户端设备类型", example = "web")
    private String devType;

    /**
     * app版本信息，web端就上报前端版本，其实觉得没有必要上报前端版本
     */
    @ApiModelProperty(value = "客户端版本", example = "4.5")
    private String appVer;

    /**
     * 客户端类型(web、android、ios)
     */
    @ApiModelProperty(value = "客户端类型", example = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)")
    private String userAgent;

    /**
     * 服务端接收到上报数据的时间
     */
    @ApiModelProperty(hidden = true)
    private String uploadTime;

    /**
     * 移动端上报批量上报用户浏览信息的条目列表
     */
    @ApiModelProperty(value = "移动端上报批量上报用户浏览信息的条目列表")
    private List<ViewEntry> innerEntries;

    public MobilePageView() {
    }


    public static MobilePageView fromString(String string) {
        return gson.fromJson(string, MobilePageView.class);
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Timestamp getEpoch() {
        return epoch;
    }

    public void setEpoch(Timestamp epoch) {
        this.epoch = epoch;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getViewId() {
        return viewId;
    }

    public void setViewId(String viewId) {
        this.viewId = viewId;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
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

    public String getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(String uploadTime) {
        this.uploadTime = uploadTime;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public List<ViewEntry> getInnerEntries() {
        return innerEntries;
    }

    public void setInnerEntries(List<ViewEntry> innerEntries) {
        this.innerEntries = innerEntries;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    @ApiModel()
    public static class ViewEntry {
        @ApiModelProperty(value = "采集信息时客户端的ip地址", example = "116.62.3.4")
        private String clientIp;

        @ApiModelProperty(value = "页面地址", example = "/index")
        private String pageUri;

        /**
         * 行业线
         */
        @ApiModelProperty(value = "行业线", example = "0")
        private int businessLine;

        /**
         * 上一个页面url
         */
        @ApiModelProperty(value = "上一个页面", example = "/main.html")
        private String referer;

        /**
         * 客户端采集时间(由于上报可能不是实时的，需要了解客户端采集的时间)，10或13位时间戳
         */
        @ApiModelProperty(value = "客户端采集时间", example = "1532413660")
        private String collectTime;

        public String getPageUri() {
            return pageUri;
        }

        public void setPageUri(String pageUri) {
            this.pageUri = pageUri;
        }

        public int getBusinessLine() {
            return businessLine;
        }

        public void setBusinessLine(int businessLine) {
            this.businessLine = businessLine;
        }

        public String getReferer() {
            return referer;
        }

        public void setReferer(String referer) {
            this.referer = referer;
        }

        public String getCollectTime() {
            return collectTime;
        }

        public void setCollectTime(String collectTime) {
            this.collectTime = collectTime;
        }

        public String getClientIp() {
            return clientIp;
        }

        public void setClientIp(String clientIp) {
            this.clientIp = clientIp;
        }
    }
}
