package cn.superid.streamer.compute;

import static cn.superid.streamer.compute.MongoConfig.readConfig;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

import cn.superid.collector.entity.view.PageStatistic;
import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.entity.view.RichPageStatistic;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
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
 * 定时从mongodb中捞数据出来计算pv uv 信息，然后保存到mongodb中相应的collection中
 *
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
    @Value("${collector.mongo.hour.rich}")
    private String hoursRich;
    @Value("${collector.mongo.day.rich}")
    private String daysRich;
    private Dataset<PageView> pageDataSet;


    @Autowired
    public RegularQuery(SparkSession sparkSession,
                        MongoTemplate mongo, @Value("${collector.mongo.page}") String pages) {
        this.mongo = mongo;
        pageDataSet = MongoSpark.load(sparkSession, readConfig(sparkSession, pages), PageView.class);
    }

    /**
     * 20分钟计算一次，防止因为重新启动的时候错过某些小时的pv uv统计
     */
    @Scheduled(fixedRate = 1000 * 60 * 20, initialDelay = 1000 * 100)
    public void everyHour() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
        repeat(hours, now, Unit.HOUR);
        System.out.println("execute everyHour of : " + now.toLocalDateTime().truncatedTo(ChronoUnit.HOURS));
        repeatRich(hoursRich, now, Unit.HOUR);
    }

    /**
     * 8小时计算一次，防止因为重新启动的时候错过某些天的pv uv统计
     */
    @Scheduled(fixedRate = 1000 * 60 * 60 * 8, initialDelay = 1000 * 10)
    public void everyDay() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
        System.out.println("execute everyDay of : " + now.toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
        repeat(days, now, Unit.DAY);
        repeatRich(daysRich, now, Unit.DAY);
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
            System.out.println("repeat size:" + size);
            for (int offset = 0; offset < size; offset++) {
                Dataset<PageView> inTimeRange = getInTimeRange(pageDataSet, last, unit, offset);
                Timestamp epoch = Timestamp.valueOf(unit.update(last, offset + 1));
                Dataset<PageStatistic> stat = inTimeRange
                        .agg(count("*").as("pv"), countDistinct(col("viewId")).as("uv"),
                                countDistinct(col("userId")).as("uvSigned"))
                        .withColumn("epoch", lit(epoch)).withColumn("id", lit(epoch.getTime()))
                        .as(Encoders.bean(PageStatistic.class));
                list.add(stat.first());
            }
            mongo.insert(list, collection);
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    /**
     * 计算每小时、每天的pv uv 放到mongodb中，如果mongodb中缺少了部分数据（程序挂了），会补全最后一条数据到当前时间之间的数据
     *
     * @param collection
     * @param now
     * @param unit
     */
    private void repeatRich(String collection, Timestamp now, Unit unit) {
        try {
            MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);
            //获取mongo中的collection最后一条文档
            Document lastDoc = mongo.getCollection(collection).find()
                    .sort(new BasicDBObject("epoch", -1))
                    .first();
            //获取最后一条文档的时间
            Timestamp last =
                    lastDoc == null ? null : Timestamp.from(lastDoc.get("epoch", Date.class).toInstant());
            ArrayList<RichPageStatistic> list = new ArrayList<>();
            int size;
            if (last == null) {
                last = Timestamp.valueOf(unit.update(now, -unit.range));
                size = unit.range;
            } else {
                //获取当前时间和最后一条mongo数据库中的时间之间的时间点个数
                size = unit.getUnit(now) - unit.getUnit(last);
                if (size < 0) {
                    size += unit.range;
                }
            }
            System.out.println("repeatRich size:" + size);
            for (int offset = 0; offset < size; offset++) {
                //从mongodb中获取last到offset+1这段时间内的数据，作为spark的dataset
                Dataset<PageView> inTimeRange = getInTimeRange(pageDataSet, last, unit, offset);
                //将last更新为offset+1的时间点
                Timestamp epoch = Timestamp.valueOf(unit.update(last, offset + 1));
                //不加Object[]强制转换，jenkins编译会报错
                if (((Object[]) inTimeRange.collect()).length > 0) {
                    Dataset<RichPageStatistic> stat = inTimeRange
                            .groupBy(inTimeRange.col("deviceType"),
                                    inTimeRange.col("allianceId"),
                                    inTimeRange.col("affairId"),
                                    inTimeRange.col("targetId"),
                                    inTimeRange.col("publicIp"))
                            .agg(count("*").as("pv"), countDistinct(col("viewId")).as("uv"),
                                    countDistinct(col("userId")).as("uvSigned"))
                            .withColumn("epoch", lit(epoch)).withColumn("id", lit(epoch.getTime()))
                            .as(Encoders.bean(RichPageStatistic.class));
                    //分组计算的结果都保存进去
                    list.addAll(stat.collectAsList());
                }

            }
            System.out.println("insert "+list +" into mongo collection : "+collection);
            mongo.insert(list, collection);
        } catch (Exception e) {
            logger.error("", e);
        }
    }


    private Dataset<PageView> getInTimeRange(Dataset<PageView> pages, Timestamp epoch, Unit unit, int offset) {
        LocalDateTime lower = unit.update(epoch, offset);
        LocalDateTime upper = unit.update(epoch, offset + 1);
        Timestamp low = Timestamp.valueOf(lower);
        Timestamp up = Timestamp.valueOf(upper);
        return pages.where(col("epoch").between(low, up));
    }

}
