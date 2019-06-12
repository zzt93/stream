package cn.superid.streamer.entity;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 不同平台访客趋势
 */
public class PlatformStatistic implements Serializable {
    private long id;
    private Timestamp epoch;

    private long web;
    private long android;
    private long ios;
    private long others;

    public PlatformStatistic() {
    }

    public PlatformStatistic(Timestamp epoch) {
        this.epoch = epoch;
        setId(epoch.getTime());
    }

    public PlatformStatistic(Timestamp epoch, long web, long android, long ios, long others) {
        this.epoch = epoch;
        this.web = web;
        this.android = android;
        this.ios = ios;
        this.others = others;
        setId(epoch.getTime());
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

    public long getWeb() {
        return web;
    }

    public void setWeb(long web) {
        this.web = web;
    }

    public long getAndroid() {
        return android;
    }

    public void setAndroid(long android) {
        this.android = android;
    }

    public long getIos() {
        return ios;
    }

    public void setIos(long ios) {
        this.ios = ios;
    }

    public long getOthers() {
        return others;
    }

    public void setOthers(long others) {
        this.others = others;
    }
}
