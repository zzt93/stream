package cn.superid.streamer.vo;

import cn.superid.collector.entity.view.PageView;

public class StatsDetailVO {
    private String clientIp;
    private String device;//浏览器
    private String uploadTime;//访问时间
    private String pageUri;//访问页面

    public StatsDetailVO() {
    }

    public StatsDetailVO(PageView pageView){
        this.clientIp = pageView.getClientIp();

        String devType = pageView.getDevType().toLowerCase();
        if (devType.equals("android") || devType.equals("ios")){
            this.device = pageView.getAppVer();
        } else {
            this.device = pageView.getDevice();
        }

        this.uploadTime = pageView.getUploadTime();
        this.pageUri = pageView.getPageUri();
    }

    public StatsDetailVO(String clientIp, String device, String uploadTime, String pageUri) {
        this.clientIp = clientIp;
        this.device = device;
        this.uploadTime = uploadTime;
        this.pageUri = pageUri;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(String uploadTime) {
        this.uploadTime = uploadTime;
    }

    public String getPageUri() {
        return pageUri;
    }

    public void setPageUri(String pageUri) {
        this.pageUri = pageUri;
    }
}
