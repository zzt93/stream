package cn.superid.streamer.compute;

import static cn.superid.streamer.compute.MongoConfig.readConfig;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

import cn.superid.streamer.constant.AuthType;
import cn.superid.streamer.dao.UserInfoLogDao;
import cn.superid.streamer.entity.AuthStatistic;
import cn.superid.streamer.entity.PageStatistic;
import cn.superid.collector.entity.view.PageView;
import cn.superid.streamer.entity.PlatformStatistic;
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

    private Dataset<PageView> pageDataSet;

    @Autowired
    private UserInfoLogDao userInfoLogDao;


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
        logger.debug("execute everyHour of :  {}", now.toLocalDateTime());
        repeat(hours, now, Unit.HOUR);
    }

    /**
     * 每天计算一次天的pv uv统计
     */
    @Scheduled(fixedRate = 1000 * 60 * 60 * 24, initialDelay = 1000 * 300)
    public void everyDay() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute everyDay of :  {}", now.toLocalDateTime());
        repeat(days, now, Unit.DAY);
    }

    @Scheduled(fixedRate = 1000L * 60 * 60 * 24 * 30, initialDelay = 1000 * 600)
    public void everyMonth() {
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute everyMonth of :  {}", now.toLocalDateTime());
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


    @Scheduled(fixedRate = 1000 * 60 * 60, initialDelay = 1000 * 60)
    public void platformEveryHour(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime());
        repeatPlatform(platformHours, now, Unit.HOUR);
    }

    @Scheduled(fixedRate = 1000 * 60 * 60 * 24, initialDelay = 1000 * 500)
    public void platformEveryDay(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime());
        repeatPlatform(platformDays, now, Unit.DAY);
    }

    @Scheduled(fixedRate = 1000L * 60 * 60 * 24 * 30, initialDelay = 1000 * 800)
    public void platformEveryMonth(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS));
        logger.debug("execute platformEveryHour of :  {}", now.toLocalDateTime());
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
                        .withColumn("id", lit(epoch.getTime()))
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

    @Scheduled(fixedRate = 1000 * 60 * 60, initialDelay = 1000 * 90)
    public void authEveryHour(){
        Timestamp now = Timestamp
                .valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
        logger.debug("execute authEveryHour of :  {}", now.toLocalDateTime());
        repeatAuth(authHours, now, Unit.HOUR);
    }

    private void repeatAuth(String collection, Timestamp now, Unit unit){
        try {
            MongoConfig.createIfNotExist(mongo, collection, unit.range * 10);

            long notAuth = userInfoLogDao.countByAuthType(AuthType.NOT_AUTH);
            long idAuth = userInfoLogDao.countByAuthType(AuthType.ID_AUTH);
            long passportAuth = userInfoLogDao.countByAuthType(AuthType.PASSPORT_AUTH);

            AuthStatistic authStatistic = new AuthStatistic(new Timestamp(new Date().getTime()));
            authStatistic.setNotAuth(notAuth);
            authStatistic.setIdAuth(idAuth);
            authStatistic.setPassportAuth(passportAuth);

            ArrayList<AuthStatistic> list = new ArrayList<>();
            list.add(authStatistic);

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
