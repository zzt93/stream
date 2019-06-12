package cn.superid.streamer.vo;

import java.sql.Timestamp;

public class PlatformTemp {
    private long id;
    private Timestamp epoch;

    private long uv;
    private String devType;

    public PlatformTemp() {
    }

    public PlatformTemp(Timestamp epoch) {
        this.epoch = epoch;
        this.setId(epoch.getTime());
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Timestamp getEpoch() {
        return epoch;
    }

    public void setEpoch(Timestamp epoch) {
        this.epoch = epoch;
    }

    public long getUv() {
        return uv;
    }

    public void setUv(long uv) {
        this.uv = uv;
    }

    public String getDevType() {
        return devType;
    }

    public void setDevType(String devType) {
        this.devType = devType;
    }
}
