package cn.superid.streamer.present;

import static cn.superid.streamer.util.TimestampUtils.truncate;

import cn.superid.collector.entity.PageStatistic;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.RegularQuery.Unit;
import cn.superid.streamer.compute.SqlQuery;
import cn.superid.streamer.present.query.TimeRange;
import com.mongodb.BasicDBObject;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@RestController
@RequestMapping("/streamer")
public class StreamerController {

  private static final Logger logger = LoggerFactory.getLogger(StreamerController.class);
  private final SqlQuery sqlQuery;
  private final MongoTemplate mongo;
  private final String day;
  private final String hour;
  private final String minute;

  @Autowired
  public StreamerController(SqlQuery sqlQuery, MongoTemplate mongo,
      @Value("${collector.mongo.minute}") String minute,
      @Value("${collector.mongo.hour}") String hour,
      @Value("${collector.mongo.day}") String day) {
    this.sqlQuery = sqlQuery;
    this.mongo = mongo;
    this.minute = minute;
    this.hour = hour;
    this.day = day;
    MongoConfig.createIfNotExist(mongo, this.minute, Unit.Minute.getRange());
  }

  @PostMapping("/query")
  public String query(@RequestBody String query) {
    return sqlQuery.query(query);
  }

  @PostMapping("/hour")
  public List<PageStatistic> hourStatistics(@RequestBody TimeRange range) {
    Criteria criteria = Criteria.where("epoch").gte(truncate(range.getFrom(), ChronoUnit.HOURS))
        .andOperator(Criteria.where("epoch").lt(truncate(range.getTo(), ChronoUnit.HOURS)));
    Query query = Query.query(criteria);
    return mongo.find(query, PageStatistic.class, hour);
  }

  @PostMapping("/day")
  public List<PageStatistic> dayStatistics(@RequestBody TimeRange range) {
    Criteria criteria = Criteria.where("epoch").gte(truncate(range.getFrom(), ChronoUnit.DAYS))
        .andOperator(Criteria.where("epoch").lt(truncate(range.getTo(), ChronoUnit.DAYS)));
    Query query = Query.query(criteria);
    return mongo.find(query, PageStatistic.class, day);
  }

  @PostMapping("/last30")
  public List<PageStatistic> minutesStatistic() {
    return mongo.findAll(PageStatistic.class, minute);
  }

  @PostMapping("/last1")
  public PageStatistic minute() {
    return mongo.getCollection(minute).find(PageStatistic.class)
        .sort(new BasicDBObject("epoch", -1))
        .first();
  }

}
