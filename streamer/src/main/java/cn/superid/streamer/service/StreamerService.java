package cn.superid.streamer.service;

import cn.superid.collector.entity.view.PageView;
import cn.superid.streamer.constant.ActiveStatus;
import cn.superid.streamer.constant.OperationType;
import cn.superid.streamer.dao.UserActiveLogDao;
import cn.superid.streamer.dao.UserInfoLogDao;
import cn.superid.streamer.dto.KafkaDeviceDTO;
import cn.superid.streamer.entity.RichPageStatistic;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.Unit;
import cn.superid.streamer.entity.UserActiveLogEntity;
import cn.superid.streamer.form.RichForm;
import cn.superid.streamer.vo.CurrentInfoVO;
import com.mongodb.spark.MongoSpark;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author dufeng
 * @create: 2018-10-15 10:56
 */
@Service
public class StreamerService {

  public static final String EPOCH = "EPOCH";
  /**
   * 限制前端查询时间范围内的时间点个数
   */
  private static final int COUNT_LIMIT = 100;
  private static final ExecutorService service = Executors.newCachedThreadPool();
  public static final String LOW = "LOW";
  public static final String UPPER = "UPPER";
  private final Logger logger = LoggerFactory.getLogger(StreamerService.class);
  private final Dataset<PageView> pageView;

  @Autowired
  private UserInfoLogDao userInfoLogDao;

  @Autowired
  private UserActiveLogDao userActiveLogDao;

  @Autowired
  public StreamerService(SparkSession spark, @Value("${collector.mongo.page}") String pages) {
    //把mongodb中的数据加载到spark中
    pageView = MongoSpark.load(spark, MongoConfig.readConfig(spark, pages), PageView.class);
    //创建spark的临时视图，用于查询
    pageView.createOrReplaceTempView(pages);
  }

  public List<RichPageStatistic> rangeRichPageviewsInUnit(@RequestBody RichForm richForm,
      Unit unit) {
    LocalDateTime from = unit.truncate(richForm.getFrom().toLocalDateTime());

    long timeDiff = unit.diff(richForm.getFrom(), richForm.getTo());
    if (timeDiff > COUNT_LIMIT) {
      logger.error("查询时间范围内包含的{}过多:{}", unit, timeDiff);
      return Collections.emptyList();
    }

    return getRichPageStatistics(from, (int) timeDiff, unit, richForm);
  }


  private List<RichPageStatistic> getRichPageStatistics(LocalDateTime dateTime, int timeDiff,
      Unit unit, RichForm query) {

    StringBuilder fromClause = new StringBuilder(" from pages where publicIp = 1 and epoch > '"
        + LOW + "' and epoch < '" + UPPER + "'");
    StringBuilder select = new StringBuilder(
        "select count(*) as pv, count(distinct viewId) as uv, count(distinct userId) as uvSigned, '"
            + EPOCH + "'");
    if (query.getAffairId() != null) {
      select.append(", ").append(query.getAffairId());
      fromClause.append(" and affairId = ").append(query.getAffairId());
    }
    if (query.getTargetId() != null) {
      select.append(", ").append(query.getTargetId());
      fromClause.append(" and targetId = ").append(query.getTargetId());
    }
    if (!StringUtils.isEmpty(query.getDevType())) {
      select.append(", '").append(query.getDevType()).append("'");
      fromClause.append(" and deviceType = '").append(query.getDevType()).append("'");
    }
    Timestamp from = Timestamp.valueOf(dateTime);
    List<Future<Row>> futures = new ArrayList<>(timeDiff);
    for (int offset = 0; offset < timeDiff; offset++) {
      Timestamp low = Timestamp.valueOf(unit.update(from, offset));
      Timestamp upper = Timestamp.valueOf(unit.update(from, offset + 1));
      futures.add(service.submit(() -> {
        String fromStr = fromClause.toString().replace(LOW, low.toString())
            .replace(UPPER, upper.toString());
        String selectStr = select.toString().replace(EPOCH, upper.toString());
            return pageView.sqlContext().sql(selectStr + fromStr).first();
          }
      ));
    }
    List<RichPageStatistic> res = new ArrayList<>(timeDiff);
    for (Future<Row> future : futures) {
      try {
        Row row = future.get();
        logger.debug("{}", row);
        RichPageStatistic stat = new RichPageStatistic(row.getString(3), row.getLong(0),
            row.getLong(1), row.getLong(2));
        res.add(stat);
      } catch (InterruptedException | ExecutionException e) {
        logger.error("", e);
      }
    }
    return res;
  }

  public CurrentInfoVO getCurrentInfo(){
    long onlineUser = userActiveLogDao.countOnlineUser();
    Timestamp from = Timestamp
            .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
    long newUser = userInfoLogDao.countByCreateTimeAfter(from);
    long activeUser = userActiveLogDao.countActiveUser(from, Timestamp.valueOf(LocalDateTime.now()));
    long totalUser = userInfoLogDao.count();

    return new CurrentInfoVO(onlineUser, newUser, activeUser, totalUser);
  }

  public void userOperation(KafkaDeviceDTO deviceDTO, int operateType, long timestamp) {
    UserActiveLogEntity userActiveLogEntity = userActiveLogDao.findByUserIdAndDeviceId(deviceDTO.getUserId(), deviceDTO.getDeviceId());
    if (userActiveLogEntity == null) {
      userActiveLogEntity = new UserActiveLogEntity();
      userActiveLogEntity.setUserId(deviceDTO.getUserId());
      userActiveLogEntity.setDeviceId(deviceDTO.getDeviceId());
      userActiveLogEntity.setAgent(deviceDTO.getAgent());
    }
    if (operateType == OperationType.online) {
      userActiveLogEntity.setLoginTime(new Timestamp(timestamp));
    } else {
      userActiveLogEntity.setLogoutTime(new Timestamp(timestamp));
    }

    userActiveLogDao.save(userActiveLogEntity);
  }

}
