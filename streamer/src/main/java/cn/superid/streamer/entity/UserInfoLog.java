package cn.superid.streamer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.sql.Timestamp;

@Table
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserInfoLog {
    private long id;
    private long userId;
    private int authType;
    private Timestamp authTime;
    private Timestamp loginTime;
    private Timestamp logoutTime;
    private Timestamp createTime;
}
