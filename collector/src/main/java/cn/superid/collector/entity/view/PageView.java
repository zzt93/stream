package cn.superid.collector.entity.view;

import cn.superid.collector.util.DevUtil;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.google.gson.Gson;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 用户浏览信息
 *
 * @author zzt
 */
@ApiModel()
public class PageView implements Serializable {
    /**
     * 内网ip正则
     */
    private static final Pattern INNER_IP = Pattern.compile("(^10\\..*)|(^172\\.(1[6-9]|2[0-9]|3[01]))|(^192\\.168)|(^127\\.)");

    private static final Gson gson = new Gson();
    @ApiModelProperty(value = "客户端ip地址", example = "116.62.3.4")
    private String clientIp;
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
    @ApiModelProperty(value = "页面地址", example = "/index")
    private String pageUri;
    @ApiModelProperty(hidden = true)
    private String serverIp;
    @ApiModelProperty(value = "用户id", example = "54321")
    private long userId;
    @ApiModelProperty(hidden = true)
    private String domain;

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
    @ApiModelProperty(value = "客户端采集时间", example = "1532413668")
    private String collectTime;
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
     * 客户端类型
     */
    @ApiModelProperty(value = "客户端类型", example = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)")
    private String userAgent;

    /**
     * 存放当前页面涉及的资源信息，比如事务id、目标id
     */
    @ApiModelProperty(value = "资源id信息", example = "{'affairId':123,'targetId':456}")
    private Map<String, Long> resources;

    /**
     * 设备类型：iphone、android、mac、windows、ios(iPad是什么样的？)
     */
    @ApiModelProperty(hidden = true)
    private String deviceType;
    /**
     * 客户端ip是否是公网ip
     */
    @ApiModelProperty(hidden = true)
    private boolean publicIp = false;

    /**
     * 事务id
     */
    @ApiModelProperty(hidden = true)
    private long affairId;
    /**
     * 目标id
     */
    @ApiModelProperty(hidden = true)
    private long targetId;
    /**
     * 盟id
     */
    @ApiModelProperty(hidden = true)
    private long allianceId;

    /**
     * 服务端接收到上报数据的时间
     */
    @ApiModelProperty(hidden = true)
    private String uploadTime;

    /**
     * 前端传过来之后，处理一下字段信息，根据前端传递的信息，来填补publicIp、deviceType字段
     */
    public void postProcess() {
        if (clientIp != null) {
            publicIp = !INNER_IP.matcher(clientIp).find();
        }

        deviceType = DevUtil.getDeviceType(this.userAgent);

        if (!CollectionUtils.isEmpty(resources)) {
            affairId = resources.get("affairId") == null ? 0 : resources.get("affairId");
            targetId = resources.get("targetId") == null ? 0 : resources.get("targetId");
            allianceId = resources.get("allianceId") == null ? 0 : resources.get("allianceId");
        }
    }


    public PageView() {
    }

    public static PageView fromString(String string) {
        return gson.fromJson(string, PageView.class);
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

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
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

    public Map<String, Long> getResources() {
        return resources;
    }

    public void setResources(Map<String, Long> resources) {
        this.resources = resources;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public boolean isPublicIp() {
        return publicIp;
    }

    public void setPublicIp(boolean publicIp) {
        this.publicIp = publicIp;
    }

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

    public long getAllianceId() {
        return allianceId;
    }

    public void setAllianceId(long allianceId) {
        this.allianceId = allianceId;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public static class PageBuilder {

        private String id;
        private String pageUri;
        private long userId;
        private Timestamp epoch;
        private String dev;
        private String domain;
        private String serverIp;
        private String clientIp;
        private int businessLine;
        private String referer;
        private String collectTime;
        private String devType;
        private String appVer;
        private String uploadTime;
        private String userAgent;
        private Map<String,Long> resources;

        public PageView build() {
            PageView pageView = new PageView();
            pageView.setViewId(id);
            pageView.setUserId(userId);
            pageView.setPageUri(pageUri);
            pageView.setEpoch(epoch);
            pageView.setDevice(dev);
            pageView.setDomain(domain);
            pageView.setClientIp(clientIp);
            pageView.setServerIp(serverIp);
            pageView.setBusinessLine(businessLine);
            pageView.setReferer(referer);
            pageView.setCollectTime(collectTime);
            pageView.setDevType(devType);
            pageView.setAppVer(appVer);
            pageView.setUploadTime(uploadTime);
            pageView.setUserAgent(userAgent);
            pageView.setResources(resources);
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

        public PageBuilder setUserId(long userId) {
            this.userId = userId;
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

        public PageBuilder setBusinessLine(int businessLine) {
            this.businessLine = businessLine;
            return this;
        }

        public PageBuilder setReferer(String referer) {
            this.referer = referer;
            return this;
        }

        public PageBuilder setCollectTime(String collectTime) {
            this.collectTime = collectTime;
            return this;
        }

        public PageBuilder setDevType(String devType) {
            this.devType = devType;
            return this;
        }

        public PageBuilder setAppVer(String appVer) {
            this.appVer = appVer;
            return this;
        }

        public PageBuilder setUploadTime(String uploadTime) {
            this.uploadTime = uploadTime;
            return this;
        }

        public PageBuilder setUserAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public PageBuilder setEpoch(Timestamp epoch) {
            this.epoch = epoch;
            return this;
        }

        public PageBuilder setDev(String dev) {
            this.dev = dev;
            return this;
        }

        public PageBuilder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public PageBuilder setResources(Map<String, Long> resources) {
            this.resources = resources;
            return this;
        }
    }
}

