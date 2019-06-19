package cn.superid.streamer.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Table(name = "user_info_log")
@Entity
public class UserInfoLogEntity {
    @Id
    private long id; // 用户id
    private int authType;
    private Timestamp authTime;
    private Timestamp createTime;

    public UserInfoLogEntity() {
    }

    public UserInfoLogEntity(long id, int authType, Timestamp authTime, Timestamp createTime) {
        this.id = id;
        this.authType = authType;
        this.authTime = authTime;
        this.createTime = createTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }
}
