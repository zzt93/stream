package cn.superid.streamer.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Table(name = "user_info_log")
@Entity
public class UserInfoLogEntity {
    @Id
    private long id;
    private long userId;
    private int authType;
    private Timestamp authTime;
    private int onlineStatus;
    private int activeStatus;
    private Timestamp createTime;

    public UserInfoLogEntity() {
    }

    public UserInfoLogEntity(long id, long userId, int authType, Timestamp authTime, int onlineStatus, int activeStatus, Timestamp createTime) {
        this.id = id;
        this.userId = userId;
        this.authType = authType;
        this.authTime = authTime;
        this.onlineStatus = onlineStatus;
        this.activeStatus = activeStatus;
        this.createTime = createTime;
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

    public int getAuthType() {
        return authType;
    }

    public void setAuthType(int authType) {
        this.authType = authType;
    }

    public Timestamp getAuthTime() {
        return authTime;
    }

    public void setAuthTime(Timestamp authTime) {
        this.authTime = authTime;
    }

    public int getOnlineStatus() {
        return onlineStatus;
    }

    public void setOnlineStatus(int onlineStatus) {
        this.onlineStatus = onlineStatus;
    }

    public int getActiveStatus() {
        return activeStatus;
    }

    public void setActiveStatus(int activeStatus) {
        this.activeStatus = activeStatus;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }
}
