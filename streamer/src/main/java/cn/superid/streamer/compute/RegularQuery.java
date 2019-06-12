package cn.superid.streamer.compute;

import static cn.superid.streamer.compute.MongoConfig.readConfig;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

import cn.superid.collector.entity.view.PageStatistic;
import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.entity.view.PlatformStatistic;
import cn.superid.collector.entity.view.RichPageStatistic;
import cn.superid.streamer.vo.LastAndSize;
import cn.superid.streamer.vo.PlatformTemp;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
    @Value("${collector.mongo.month}")
    private String months;

    @Value("{collector.mongo.platform.hour}")
    private String platformHours;
    @Value("{collector.mongo.platform.day}")
    private String platformDays;
    @Value("{collector.mongo.platform.month}")
    private String platformMonths;

    @Value("{collector.mongo.auth.hour}")
    private String authHours;
    @Value("{collector.mongo.auth.day}")
    private String authDays;
    @Value("{collector.mongo.auth.month}")
    private String authMonths;

    private Dataset<PageView> pageDataSet;


    @Autowired
    public RegularQuery(SparkSession sparkSession,
                        MongoTemplate mongo, @Value("${collector.mongo.page}") String pages) {
        this.mongo = mongo;
        pageDataSet = MongoSpark.load(sparkSession, readConfig(sparkSession, pages), PageView.class);
    }

    /**
     * 每个小时计算一次小时的pv uv统计
     */
    @Scheduled(fixedRate = 1000 * 60 * 60, initialDelay = 1000 * 30)
    public void everyHour() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
        logger.debug("execute everyHour of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.HOURS));
        repeat(hours, now, Unit.HOUR);
    }

    /**
     * 每天计算一次天的pv uv统计
     */
    @Scheduled(fixedRate = 1000 * 60 * 60 * 24, initialDelay = 1000 * 300)
    public void everyDay() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute everyDay of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
        repeat(days, now, Unit.DAY);
    }

    @Scheduled(fixedRate = 1000L * 60 * 60 * 24 * 30, initialDelay = 1000 * 600)
    public void everyMonth() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.MONTHS));
        logger.debug("execute everyMonth of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.MONTHS));
        repeat(months, now, Unit.MONTH);
    }

    private void repeat(String collection, Timestamp now, Unit unit) {
        try {
            MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);
            LastAndSize lastAndSize = getLastAndSize(collection, now, unit, "repeat");
            Timestamp last = lastAndSize.getLast();
            int size = lastAndSize.getSize();

            ArrayList<PageStatistic> list = new ArrayList<>();
            for (int offset = 0; offset < size; offset++) {
                Dataset<PageView> inTimeRange = getInTimeRange(pageDataSet, last, unit, offset);
                if (((Object[]) inTimeRange.collect()).length <= 0) {
                    continue;
                }
                Timestamp epoch = Timestamp.valueOf(unit.update(last,  offset+1));
                logger.debug("{}", epoch);
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
            LastAndSize lastAndSize = getLastAndSize(collection, now, unit, "repeatRich");
            Timestamp last = lastAndSize.getLast();
            int size = lastAndSize.getSize();

            ArrayList<RichPageStatistic> list = new ArrayList<>();
            for (int offset = 0; offset < size; offset++) {
                //从mongodb中获取last到offset+1这段时间内的数据，作为spark的dataset
                Dataset<PageView> inTimeRange = getInTimeRange(pageDataSet, last, unit, offset);
                //不加Object[]强制转换，jenkins编译会报错
                if (((Object[]) inTimeRange.collect()).length <= 0) {
                    continue;
                }
                Timestamp epoch = Timestamp.valueOf(unit.update(last,  offset+1));
                Dataset<RichPageStatistic> stat = inTimeRange
                        .groupBy(inTimeRange.col("deviceType"),
                                inTimeRange.col("allianceId"),
                                inTimeRange.col("affairId"),
                                inTimeRange.col("targetId"),
                                inTimeRange.col("publicIp"))
                        .agg(count("*").as("pv"), countDistinct(col("viewId")).as("uv"),
                                countDistinct(col("userId")).as("uvSigned"))
                        .withColumn("epoch", lit(epoch))
//                            .withColumn("id", lit(epoch.getTime())) //同一时间有多个记录，不能用时间做为id，否则插入mongo的时候报错 dup key
                        .as(Encoders.bean(RichPageStatistic.class));
                //分组计算的结果都保存进去
                list.addAll(stat.collectAsList());
                logger.debug("insert {} into mongo collection: {}", list, collection);
                mongo.insert(list, collection);
                list.clear();
                logger.debug("offset: {}", offset);
                logger.debug("repeatRich last: {}", last);
            }

        } catch (Exception e) {
            logger.error("", e);
        }
    }

    @Scheduled(fixedRate = 1000 * 60 * 60, initialDelay = 1000 * 60)
    public void platformEveryHour(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.HOURS));
        repeatPlatform(platformHours, now, Unit.HOUR);
    }

    @Scheduled(fixedRate = 1000 * 60 * 60 * 24, initialDelay = 1000 * 500)
    public void platformEveryDay(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
        repeatPlatform(platformDays, now, Unit.DAY);
    }

    @Scheduled(fixedRate = 1000L * 60 * 60 * 24 * 30, initialDelay = 1000 * 800)
    public void platformEveryMonth(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.MONTHS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime().truncatedTo(ChronoUnit.MONTHS));
        repeatPlatform(platformMonths, now, Unit.MONTH);
    }

    private void repeatPlatform(String collection, Timestamp now, Unit unit){
        try {
            MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);
            LastAndSize lastAndSize = getLastAndSize(collection, now, unit, "repeatPlatform");
            Timestamp last = lastAndSize.getLast();
            int size = lastAndSize.getSize();

            ArrayList<PlatformStatistic> list = new ArrayList<>();
            for (int offset = 0; offset < size; offset++) {
                Dataset<PageView> inTimeRange = getInTimeRange(pageDataSet, last, unit, offset);
                if (((Object[]) inTimeRange.collect()).length <= 0) {
                    continue;
                }
                Timestamp epoch = Timestamp.valueOf(unit.update(last,  offset+1));
                Dataset<PlatformTemp> stats = inTimeRange
                        .groupBy(inTimeRange.col("devType"))
                        .agg(countDistinct("viewId").as("uv"))
                        .withColumn("epoch", lit(epoch))
                        .as(Encoders.bean(PlatformTemp.class));
                List<PlatformTemp> platformTempList = stats.collectAsList();
                if (platformTempList.size() <= 0) {
                    continue;
                }
                PlatformStatistic platformStatistic = new PlatformStatistic(platformTempList.get(0).getEpoch(), 0L, 0L, 0L, 0L);
                for (PlatformTemp p : platformTempList) {
                    String devType = p.getDevType().toLowerCase();
                    if (devType.equals("web")) {
                        platformStatistic.setWeb(platformStatistic.getWeb()+p.getUv());
                    } else if (devType.equals("android")) {
                        platformStatistic.setAndroid(platformStatistic.getAndroid()+p.getUv());
                    } else if (devType.equals("ios")) {
                        platformStatistic.setIos(platformStatistic.getIos()+p.getUv());
                    } else {
                        platformStatistic.setOthers(platformStatistic.getOthers()+p.getUv());
                    }
                }
                list.add(platformStatistic);
            }
            mongo.insert(list, collection);
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void repeatAuth(String collection, Timestamp now, Unit unit){
        try {
            MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);
            LastAndSize lastAndSize = getLastAndSize(collection, now, unit, "repeatAuth");
            Timestamp last = lastAndSize.getLast();
            int size = lastAndSize.getSize();

            ArrayList<PageStatistic> list = new ArrayList<>();
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

    private LastAndSize getLastAndSize(String collection, Timestamp now, Unit unit, String caller){
        //获取mongo中的collection最后一条文档
        Document lastDoc = mongo.getCollection(collection).find()
                .sort(new BasicDBObject("epoch", -1))
                .first();
        //获取最后一条文档的时间
        Timestamp last =
                lastDoc == null ? null : Timestamp.from(lastDoc.get("epoch", Date.class).toInstant());
        logger.debug("{} last: {}", caller, last);
        int size;
        if (last == null) {
            last = Timestamp.valueOf(unit.update(now, -unit.range));
            size = unit.range;
        } else {
            //获取当前时间和最后一条mongo数据库中的时间之间的时间点个数
            size = unit.diff(now, last);
            if (size < 0) {
                size += unit.range;
            }
        }
        logger.debug("{} last: {}", caller, last);
        logger.debug("{} size: {}", caller, size);

        return new LastAndSize(last, size);
    }

}
