package cn.superid.streamer.controller;

import static cn.superid.streamer.util.TimestampUtils.truncate;

import cn.superid.collector.entity.view.PageStatistic;
import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.entity.view.RichPageStatistic;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.Unit;
import cn.superid.streamer.compute.SqlQuery;
import cn.superid.streamer.form.RichForm;
import cn.superid.streamer.form.TimeRange;
import cn.superid.streamer.service.StreamerService;
import com.google.common.base.Preconditions;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.google.gson.Gson;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
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
@Api(value = "用户浏览及操作信息查询展示")
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
    private final String dayRich;
    private final String hourRich;
    private final String minuteRich;
    private final String page;
    @Autowired
    private StreamerService streamerService;

    @Autowired
    public StreamerController(SqlQuery sqlQuery, MongoTemplate mongo,
                              @Value("${collector.mongo.page}") String page,
                              @Value("${collector.mongo.minute}") String minute,
                              @Value("${collector.mongo.hour}") String hour,
                              @Value("${collector.mongo.day}") String day,
                              @Value("${collector.mongo.minute.rich}") String minuteRich,
                              @Value("${collector.mongo.hour.rich}") String hourRich,
                              @Value("${collector.mongo.day.rich}") String dayRich
    ) {
        this.sqlQuery = sqlQuery;
        this.mongo = mongo;
        this.minute = minute;
        this.hour = hour;
        this.day = day;
        this.minuteRich = minuteRich;
        this.hourRich = hourRich;
        this.dayRich = dayRich;
        this.page = page;
        MongoConfig.createIfNotExist(mongo, this.minute, Unit.Minute.getRange() * 50);
    }

    @PostMapping("/query")
    public String query(@RequestBody String query) {
        return sqlQuery.query(query);
    }

    /**
     * 按天查询统计信息
     *
     * @param range
     * @return
     */
    @PostMapping("/day")
    public List<PageStatistic> dayStatistics(@RequestBody TimeRange range) {
        return queryMongo(range, day, ChronoUnit.DAYS, PageStatistic.class);
    }

    /**
     * 按天查询统计信息
     *
     * @param range
     * @return
     */
    @PostMapping("/day_rich")
    public List<RichPageStatistic> dayRichStatistics(@RequestBody TimeRange range) {
        return queryMongo(range, dayRich, ChronoUnit.DAYS, RichPageStatistic.class);
    }

    /**
     * 按天查询详情
     *
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
     *
     * @param range
     * @return
     */
    @PostMapping("/hour")
    public List<PageStatistic> hourStatistics(@RequestBody TimeRange range) {
        return queryMongo(range, hour, ChronoUnit.HOURS, PageStatistic.class);
    }

    /**
     * 按小时查询统计信息
     *
     * @param range
     * @return
     */
    @PostMapping("/hour_rich")
    public List<RichPageStatistic> hourRichStatistics(@RequestBody TimeRange range) {
        return queryMongo(range, hourRich, ChronoUnit.HOURS, RichPageStatistic.class);
    }

    /**
     * 按小时查询详情
     *
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
     *
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
     *
     * @param range      时间范围
     * @param collection mongodb的集合
     * @param unit       时间单位
     * @param tClass     结果类型
     * @param <T>        范型参数
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
     *
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
     * 查询最近30分钟的浏览统计信息（刷新页面或者刚打开页面的时候，一次性加载30分钟的数据，后续只增量更新一分钟的数据）
     *
     * @return
     */
    @PostMapping("/last30rich")
    public List<RichPageStatistic> minutesRichStatistic() {
        LocalDateTime truncate = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES);
        Criteria criteria = Criteria.where("epoch")
                .gt(Timestamp.valueOf(truncate.minusMinutes(MINUTES_COUNT)))
                .andOperator(Criteria.where("epoch").lte(Timestamp.valueOf(truncate)));
        Query query = Query.query(criteria).limit(MINUTES_COUNT).with(Sort.by(Direction.ASC, "epoch"));
        LinkedList<RichPageStatistic> pageStatistics = new LinkedList<>(mongo.find(query, RichPageStatistic.class, minuteRich));
        if (pageStatistics.size() != MINUTES_COUNT) {
            ListIterator<RichPageStatistic> it = pageStatistics.listIterator();
            for (int i = MINUTES_COUNT - 1; i >= 0; i--) {
                LocalDateTime time = truncate.minusMinutes(i);
                boolean hasMore = it.hasNext();
                if (hasMore && time.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli() == it.next().getId()) {
                } else {
                    if (it.hasPrevious() && hasMore) {
                        it.previous();
                    }
                    RichPageStatistic pageStatistic = new RichPageStatistic(Timestamp.valueOf(time));
                    it.add(pageStatistic);
                }
            }
        }
        Preconditions.checkState(pageStatistics.size() == MINUTES_COUNT, "Wrong logic");
        return pageStatistics;
    }

    /**
     * 前端页面每一分钟调用一次接口，获取最新的一分钟的页面浏览统计信息
     *
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

    /**
     * 前端页面每一分钟调用一次接口，获取最新的一分钟的页面浏览统计信息
     *
     * @return
     */
    @PostMapping("/last1rich")
    public RichPageStatistic minuteRich() {
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        Criteria criteria = Criteria.where("epoch").is(truncate(now, ChronoUnit.MINUTES));
        Query query = Query.query(criteria);
        RichPageStatistic one = mongo.findOne(query, RichPageStatistic.class, minuteRich);
        return one == null ? new RichPageStatistic(now) : one;
    }


    /**
     * 指定范围内的更多维度的pv uv 信息
     *
     * @return
     */
    @ApiOperation(value = "按照事务id、目标id、时间范围、用户设备类型查看pv uv", notes = "", response = RichPageStatistic.class)
    @PostMapping("/range/rich/pageview")
    public List<RichPageStatistic> rangeRichPageviews(@RequestBody RichForm richForm) {
        richForm.validate();
        System.out.println("request info : " + new Gson().toJson(richForm));
        if ("minute".equalsIgnoreCase(richForm.getTimeUnit())) {
            return streamerService.rangeRichPageviewsInMinutes(richForm);
        } else if ("hour".equalsIgnoreCase(richForm.getTimeUnit())) {
            return streamerService.rangeRichPageviewsInHours(richForm);
        } else if ("day".equalsIgnoreCase(richForm.getTimeUnit())) {
            return streamerService.rangeRichPageviewsInDays(richForm);
        } else {
            logger.error("未识别的时间单位{}", richForm.getTimeUnit());
            return Collections.emptyList();
        }
    }
}
