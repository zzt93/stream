package cn.superid.streamer.compute;

import static cn.superid.streamer.compute.SparkConfig.readConfig;
import static org.apache.spark.sql.functions.col;

import cn.superid.collector.entity.PageStatistic;
import cn.superid.collector.entity.PageView;
import cn.superid.streamer.util.TimestampUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * @author zzt
 */
@Service
public class RegularQuery implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(RegularQuery.class);
  private final String hours = "hours";
  private final String days = "days";
  private final SparkSession sparkSession;
  private final MongoTemplate mongo;
  @Value("${collector.mongo.page}")
  private String pages;
  private Dataset<PageView> pageSet;


  @Autowired
  public RegularQuery(SparkSession sparkSession,
      MongoTemplate mongo) {
    this.sparkSession = sparkSession;
    this.mongo = mongo;
    pageSet = MongoSpark.load(sparkSession, readConfig(sparkSession, this.pages), PageView.class);
  }

  @Scheduled(fixedRate = 1000 * 60)
  public void everyHour() {
    Timestamp now = Timestamp
        .valueOf(LocalDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.HOURS));
    repeat(hours, now, Unit.HOUR);
  }

  @Scheduled(fixedRate = 1000 * 60 * 24)
  public void everyDay() {
    Timestamp now = Timestamp
        .valueOf(LocalDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.DAYS));
    repeat(days, now, Unit.DAY);
  }

  private void repeat(String collection, Timestamp now, Unit unit) {
    try {
      Timestamp last = mongo.getCollection(collection).find(PageStatistic.class)
          .sort(new BasicDBObject("epoch", -1))
          .first().getEpoch();
      ArrayList<PageStatistic> list = new ArrayList<>();
      int size;
      if (last == null) {
        last = TimestampUtils.addDay(now, -unit.range - 1);
        size = unit.range;
      } else {
        size = now.toLocalDateTime().getHour() - last.toLocalDateTime().getHour() - 1;
        if (size <= 0) {
          size += unit.range;
        }
      }
      for (int offset = 1; offset <= size; offset++) {
        PageStatistic pageStatistic = new PageStatistic();
        Dataset<PageView> inTimeRange = getInTimeRange(pageSet, last, unit, offset);
        pageStatistic.setPv(inTimeRange.count());
        pageStatistic.setUv(inTimeRange.select(col("id")).distinct().count());
        pageStatistic.setUvSigned(inTimeRange.select(col("userId")).distinct().count());
        list.add(pageStatistic);
      }
      mongo.insert(list, collection);
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  private Dataset<PageView> getInTimeRange(Dataset<PageView> pages, Timestamp epoch, Unit unit,
      int offset) {
    Instant lower = unit.update(epoch.toLocalDateTime(), offset).toInstant(ZoneOffset.UTC);
    Instant upper = unit.update(epoch.toLocalDateTime(), offset+1).toInstant(ZoneOffset.UTC);
    Timestamp low = Timestamp.from(lower), up = Timestamp.from(upper);
    return pages.where(col("epoch").between(low, up));
  }


  public enum Unit {
    DAY(31) {
      @Override
      public LocalDateTime update(LocalDateTime dateTime, int offset) {
        return dateTime.plusDays(offset);
      }
    }, HOUR(24) {
      @Override
      public LocalDateTime update(LocalDateTime dateTime, int offset) {
        return dateTime.plusHours(offset);
      }
    };

    private final int range;

    Unit(int range) {
      this.range = range;
    }


    public int getRange() {
      return range;
    }

    public abstract LocalDateTime update(LocalDateTime dateTime, int offset);
  }

}
