package cn.superid.streamer.dao;

import cn.superid.streamer.entity.UserActiveLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public interface UserActiveLogDao extends JpaRepository<UserActiveLogEntity, Long> {
    /**
     * 获取指定时间范围内在线的用户人数
     * @return
     */
    @Query(value = "select count(distinct user_id) from user_active_log where " +
            "(login_time is not null and login_time between ?1 and ?2)" +
            "or (logout_time is not null and logout_time between ?1 and ?2)" +
            "or (login_time is not null and login_time < ?1" +
            "and (logout_time is null or logout_time < login_time))", nativeQuery = true)
    long countActiveUser(Timestamp from, Timestamp to);

    /**
     * 获取当前在线用户
     * @return
     */
    @Query(value = "select count(distinct user_id) from user_active_log where " +
            "(login_time is not null and login_time <= current_time)" +
            "and (logout_time is null or logout_time < login_time)", nativeQuery = true)
    long countOnlineUser();

    UserActiveLogEntity findByUserIdAndDeviceId(long userId, String deviceId);
}
