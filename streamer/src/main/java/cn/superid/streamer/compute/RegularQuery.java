package cn.superid.streamer.compute;

import static cn.superid.streamer.compute.MongoConfig.readConfig;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

import cn.superid.collector.entity.PageStatistic;
import cn.superid.collector.entity.PageView;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
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
  private final MongoTemplate mongo;
  @Value("${collector.mongo.hour}")
  private String hours;
  @Value("${collector.mongo.day}")
  private String days;
  private Dataset<PageView> pageSet;


  @Autowired
  public RegularQuery(SparkSession sparkSession,
      MongoTemplate mongo, @Value("${collector.mongo.page}") String pages) {
    this.mongo = mongo;
    pageSet = MongoSpark.load(sparkSession, readConfig(sparkSession, pages), PageView.class);
  }

  @Scheduled(fixedRate = 1000 * 60, initialDelay = 1000 * 100)
  public void everyHour() {
    Timestamp now = Timestamp
        .valueOf(LocalDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.HOURS));
    repeat(hours, now, Unit.HOUR);
  }

  @Scheduled(fixedRate = 1000 * 60 * 24, initialDelay = 1000 * 10)
  public void everyDay() {
    Timestamp now = Timestamp
        .valueOf(LocalDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.DAYS));
    repeat(days, now, Unit.DAY);
  }

  private void repeat(String collection, Timestamp now, Unit unit) {
    try {
      MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);
      Document lastDoc = mongo.getCollection(collection).find()
          .sort(new BasicDBObject("epoch", -1))
          .first();
      Timestamp last =
          lastDoc == null ? null : Timestamp.from(lastDoc.get("epoch", Date.class).toInstant());
      ArrayList<PageStatistic> list = new ArrayList<>();
      int size;
      if (last == null) {
        last = Timestamp.valueOf(unit.update(now, -unit.range));
        size = unit.range;
      } else {
        size = unit.getUnit(now) - unit.getUnit(last);
        if (size < 0) {
          size += unit.range;
        }
      }
      for (int offset = 0; offset < size; offset++) {
        Dataset<PageView> inTimeRange = getInTimeRange(pageSet, last, unit, offset);
        Timestamp epoch = Timestamp.valueOf(unit.update(last, offset + 1));
        Dataset<PageStatistic> stat = inTimeRange
            .agg(count("*").as("pv"), countDistinct(col("viewId")).as("uv"),
                countDistinct(col("userId")).as("uvSigned"))
            .withColumn("epoch", lit(epoch))
            .as(Encoders.bean(PageStatistic.class));
        list.add(stat.first());
      }
      mongo.insert(list, collection);
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  private Dataset<PageView> getInTimeRange(Dataset<PageView> pages, Timestamp epoch, Unit unit,
      int offset) {
    LocalDateTime lower = unit.update(epoch, offset);
    LocalDateTime upper = unit.update(epoch, offset + 1);
    Timestamp low = Timestamp.valueOf(lower), up = Timestamp.valueOf(upper);
    return pages.where(col("epoch").between(low, up));
  }


  public enum Unit {
    DAY(31) {
      @Override
      public LocalDateTime update(Timestamp dateTime, int offset) {
        return dateTime.toLocalDateTime().plusDays(offset);
      }

      @Override
      public int getUnit(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getDayOfMonth();
      }
    }, HOUR(24) {
      @Override
      public LocalDateTime update(Timestamp dateTime, int offset) {
        return dateTime.toLocalDateTime().plusHours(offset);
      }

      @Override
      public int getUnit(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getHour();
      }
    }, Minute(30) {
      @Override
      public LocalDateTime update(Timestamp dateTime, int offset) {
        return dateTime.toLocalDateTime().plusMinutes(offset);
      }

      @Override
      public int getUnit(Timestamp timestamp) {
        return timestamp.toLocalDateTime().getMinute();
      }
    };

    private final int range;

    Unit(int range) {
      this.range = range;
    }

    public int getRange() {
      return range;
    }

    public abstract LocalDateTime update(Timestamp dateTime, int offset);

    public abstract int getUnit(Timestamp timestamp);
  }

}
