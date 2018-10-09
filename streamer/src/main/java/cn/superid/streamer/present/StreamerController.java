package cn.superid.streamer.present;

import static cn.superid.streamer.util.TimestampUtils.truncate;

import cn.superid.collector.entity.view.PageStatistic;
import cn.superid.collector.entity.view.PageView;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.RegularQuery.Unit;
import cn.superid.streamer.compute.SqlQuery;
import cn.superid.streamer.present.query.TimeRange;
import com.google.common.base.Preconditions;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@RestController
@RequestMapping("/streamer")
@CrossOrigin(origins = "*")
public class StreamerController {

  public static final int MINUTES_COUNT = 30;
  private static final Logger logger = LoggerFactory.getLogger(StreamerController.class);
  private final SqlQuery sqlQuery;
  private final MongoTemplate mongo;
  private final String day;
  private final String hour;
  private final String minute;
  private final String page;

  @Autowired
  public StreamerController(SqlQuery sqlQuery, MongoTemplate mongo,
      @Value("${collector.mongo.page}") String page,
      @Value("${collector.mongo.minute}") String minute,
      @Value("${collector.mongo.hour}") String hour,
      @Value("${collector.mongo.day}") String day) {
    this.sqlQuery = sqlQuery;
    this.mongo = mongo;
    this.minute = minute;
    this.hour = hour;
    this.day = day;
    this.page = page;
    MongoConfig.createIfNotExist(mongo, this.minute, Unit.Minute.getRange() * 50);
  }

  @PostMapping("/query")
  public String query(@RequestBody String query) {
    return sqlQuery.query(query);
  }

  /**
   * 按天查询统计信息
   * @param range
   * @return
   */
  @PostMapping("/day")
  public List<PageStatistic> dayStatistics(@RequestBody TimeRange range) {
    return queryMongo(range, day, ChronoUnit.DAYS, PageStatistic.class);
  }

  /**
   * 按天查询详情
   * @param range
   * @return
   */
  @PostMapping("/day/detail")
  public List<PageView> dayDetail(@RequestBody TimeRange range) {
    Preconditions.checkNotNull(range.pageRequest(), "No pagination info or too deep pagination");
    return queryMongo(range, page, ChronoUnit.DAYS, PageView.class);
  }

  /**
   * 按小时查询统计信息
   * @param range
   * @return
   */
  @PostMapping("/hour")
  public List<PageStatistic> hourStatistics(@RequestBody TimeRange range) {
    return queryMongo(range, hour, ChronoUnit.HOURS, PageStatistic.class);
  }

  /**
   * 按小时查询详情
   * @param range
   * @return
   */
  @PostMapping("/hour/detail")
  public List<PageView> hourDetail(@RequestBody TimeRange range) {
    Preconditions.checkNotNull(range.pageRequest(), "No pagination info or too deep pagination");
    return queryMongo(range, page, ChronoUnit.HOURS, PageView.class);
  }

  /**
   * 按分钟查询详情
   * @param range
   * @return
   */
  @PostMapping("/minute/detail")
  public List<PageView> minuteDetail(@RequestBody TimeRange range) {
    Preconditions.checkNotNull(range.pageRequest(), "No pagination info or too deep pagination");
    return queryMongo(range, page, ChronoUnit.MINUTES, PageView.class);
  }

  /**
   * 从mongodb中查询指定"集合"中的某个时间范围内的数据
   * @param range 时间范围
   * @param collection mongodb的集合
   * @param unit 时间单位
   * @param tClass 结果类型
   * @param <T> 范型参数
   * @return
   */
  private <T> List<T> queryMongo(TimeRange range, String collection, ChronoUnit unit, Class<T> tClass) {
    Preconditions.checkArgument(range.getFrom() != null && range.getTo() != null, "No time range");
    Criteria criteria = Criteria.where("epoch").gte(truncate(range.getFrom(), unit))
        .andOperator(Criteria.where("epoch").lt(truncate(range.getTo(), unit)));
    Query query = Query.query(criteria).with(range.pageRequest());
    return mongo.find(query, tClass, collection);
  }

  /**
   * 查询最近30分钟的浏览统计信息（刷新页面或者刚打开页面的时候，一次性加载30分钟的数据，后续只增量更新一分钟的数据）
   * @return
   */
  @PostMapping("/last30")
  public List<PageStatistic> minutesStatistic() {
    LocalDateTime truncate = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES);
    Criteria criteria = Criteria.where("epoch")
        .gt(Timestamp.valueOf(truncate.minusMinutes(MINUTES_COUNT)))
        .andOperator(Criteria.where("epoch").lte(Timestamp.valueOf(truncate)));
    Query query = Query.query(criteria).limit(MINUTES_COUNT).with(Sort.by(Direction.ASC, "epoch"));
    LinkedList<PageStatistic> pageStatistics = new LinkedList<>(mongo.find(query, PageStatistic.class, minute));
    if (pageStatistics.size() != MINUTES_COUNT) {
      ListIterator<PageStatistic> it = pageStatistics.listIterator();
      for (int i = MINUTES_COUNT - 1; i >= 0; i--) {
        LocalDateTime time = truncate.minusMinutes(i);
        boolean hasMore = it.hasNext();
        if (hasMore && time.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli() == it.next().getId()) {
        } else {
          if (it.hasPrevious() && hasMore) {
            it.previous();
          }
          PageStatistic pageStatistic = new PageStatistic(Timestamp.valueOf(time));
          it.add(pageStatistic);
        }
      }
    }
    Preconditions.checkState(pageStatistics.size() == MINUTES_COUNT, "Wrong logic");
    return pageStatistics;
  }

  /**
   * 前端页面每一分钟调用一次接口，获取最新的一分钟的页面浏览统计信息
   * @return
   */
  @PostMapping("/last1")
  public PageStatistic minute() {
    Timestamp now = Timestamp.valueOf(LocalDateTime.now());
    Criteria criteria = Criteria.where("epoch").is(truncate(now, ChronoUnit.MINUTES));
    Query query = Query.query(criteria);
    PageStatistic one = mongo.findOne(query, PageStatistic.class, minute);
    return one == null ? new PageStatistic(now) : one;
  }

}
