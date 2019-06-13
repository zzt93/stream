package cn.superid.streamer.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Table(name = "user_info_log")
@Entity
public class UserActiveLogEntity {
    @Id
    private long id;
    private long userId;
    private String deviceId;
    private Timestamp loginTime;
    private Timestamp logoutTime;

    public UserActiveLogEntity() {
    }

    public UserActiveLogEntity(long id, long userId, String deviceId, Timestamp loginTime, Timestamp logoutTime) {
        this.id = id;
        this.userId = userId;
        this.deviceId = deviceId;
        this.loginTime = loginTime;
        this.logoutTime = logoutTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Timestamp getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Timestamp loginTime) {
        this.loginTime = loginTime;
    }

    public Timestamp getLogoutTime() {
        return logoutTime;
    }

    public void setLogoutTime(Timestamp logoutTime) {
        this.logoutTime = logoutTime;
    }
}
