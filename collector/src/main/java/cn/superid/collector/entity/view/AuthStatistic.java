package cn.superid.collector.entity.view;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 用户认证分析
 */
public class AuthStatistic implements Serializable {
    private long id;
    private Timestamp epoch;

    // 未认证
    private long notAuth;

    // 身份认证
    private long idAuth;

    // 护照认证
    private long passportAuth;

    public AuthStatistic() {
    }

    public AuthStatistic(Timestamp epoch) {
        this.epoch = epoch;
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

    public long getNotAuth() {
        return notAuth;
    }

    public void setNotAuth(long notAuth) {
        this.notAuth = notAuth;
    }

    public long getIdAuth() {
        return idAuth;
    }

    public void setIdAuth(long idAuth) {
        this.idAuth = idAuth;
    }

    public long getPassportAuth() {
        return passportAuth;
    }

    public void setPassportAuth(long passportAuth) {
        this.passportAuth = passportAuth;
    }
}
