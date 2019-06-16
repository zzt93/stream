package cn.superid.streamer.dao;

import cn.superid.streamer.entity.UserInfoLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public interface UserInfoLogDao extends JpaRepository<UserInfoLogEntity, Long> {
    long countByAuthType(int authType);

    long countByCreateTimeAfter(Timestamp timestamp);
}
