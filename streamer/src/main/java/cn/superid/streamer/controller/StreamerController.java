package cn.superid.streamer.controller;

import static cn.superid.streamer.util.TimestampUtils.truncate;

import cn.superid.collector.entity.view.*;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.SqlQuery;
import cn.superid.streamer.compute.Unit;
import cn.superid.streamer.form.RichForm;
import cn.superid.streamer.form.TimeRange;
import cn.superid.streamer.service.StreamerService;
import cn.superid.streamer.vo.CurrentInfoVO;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
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
@Api(value = "用户浏览及操作信息查询展示")
@RestController
@RequestMapping("/streamer")
@CrossOrigin(origins = "*")
public class StreamerController {

    public static final int MINUTES_COUNT = 30;
    private static final Logger logger = LoggerFactory.getLogger(StreamerController.class);
    private final SqlQuery sqlQuery;
    private final MongoTemplate mongo;
    private final String month;
    private final String day;
    private final String hour;
    private final String minute;
    private final String page;

    private final String platformHours;
    private final String platformDays;
    private final String platformMonths;

    private final String authHours;
    private final String authDays;
    private final String authMonths;

    @Autowired
    private StreamerService streamerService;

    @Autowired
    public StreamerController(SqlQuery sqlQuery, MongoTemplate mongo,
                              @Value("${collector.mongo.page}") String page,
                              @Value("${collector.mongo.minute}") String minute,
                              @Value("${collector.mongo.hour}") String hour,
                              @Value("${collector.mongo.day}") String day,
                              @Value("${collector.mongo.month}") String month,
                              @Value("{collector.mongo.platform.hour}") String platformHours,
                              @Value("{collector.mongo.platform.day}") String platformDays,
                              @Value("{collector.mongo.platform.month}") String platformMonths,
                              @Value("{collector.mongo.auth.hour}") String authHours,
                              @Value("{collector.mongo.auth.day}") String authDays,
                              @Value("{collector.mongo.auth.month}") String authMonths
    ) {
        this.sqlQuery = sqlQuery;
        this.mongo = mongo;
        this.minute = minute;
        this.hour = hour;
        this.day = day;
        this.month = month;
        this.page = page;

        this.platformHours = platformHours;
        this.platformDays = platformDays;
        this.platformMonths = platformMonths;

        this.authHours = authHours;
        this.authDays = authDays;
        this.authMonths = authMonths;

        MongoConfig.createIfNotExist(mongo, this.minute, Unit.MINUTE.getRange() * 50);
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

//    @PostMapping("/day_rich")
//    public List<RichPageStatistic> dayRichStatistics(@RequestBody TimeRange range) {
//        return queryMongo(range, dayRich, ChronoUnit.DAYS, RichPageStatistic.class);
//    }

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


//    @PostMapping("/hour_rich")
//    public List<RichPageStatistic> hourRichStatistics(@RequestBody TimeRange range) {
//        return queryMongo(range, hourRich, ChronoUnit.HOURS, RichPageStatistic.class);
//    }

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
//    @PostMapping("/last30rich")
//    public List<RichPageStatistic> minutesRichStatistic() {
//        LocalDateTime truncate = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES);
//        Criteria criteria = Criteria.where("epoch")
//                .gt(Timestamp.valueOf(truncate.minusMinutes(MINUTES_COUNT)))
//                .andOperator(Criteria.where("epoch").lte(Timestamp.valueOf(truncate)));
//        Query query = Query.query(criteria).limit(MINUTES_COUNT).with(Sort.by(Direction.ASC, "epoch"));
//        LinkedList<RichPageStatistic> pageStatistics = new LinkedList<>(mongo.find(query, RichPageStatistic.class, minuteRich));
//        if (pageStatistics.size() != MINUTES_COUNT) {
//            ListIterator<RichPageStatistic> it = pageStatistics.listIterator();
//            for (int i = MINUTES_COUNT - 1; i >= 0; i--) {
//                LocalDateTime time = truncate.minusMinutes(i);
//                boolean hasMore = it.hasNext();
//                if (hasMore && time.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli() == it.next().getId()) {
//                } else {
//                    if (it.hasPrevious() && hasMore) {
//                        it.previous();
//                    }
//                    RichPageStatistic pageStatistic = new RichPageStatistic(Timestamp.valueOf(time));
//                    it.add(pageStatistic);
//                }
//            }
//        }
//        Preconditions.checkState(pageStatistics.size() == MINUTES_COUNT, "Wrong logic");
//        return pageStatistics;
//    }

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

//    @PostMapping("/last1rich")
//    public RichPageStatistic minuteRich() {
//        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
//        Criteria criteria = Criteria.where("epoch").is(truncate(now, ChronoUnit.MINUTES));
//        Query query = Query.query(criteria);
//        RichPageStatistic one = mongo.findOne(query, RichPageStatistic.class, minuteRich);
//        return one == null ? new RichPageStatistic(now) : one;
//    }



    /**
     * 指定范围内的更多维度的pv uv 信息
     */
    @ApiOperation(value = "按照事务id、目标id、时间范围、用户设备类型查看pv uv", notes = "", response = RichPageStatistic.class)
    @PostMapping("/range/rich/pageview")
    public List<RichPageStatistic> rangeRichPageviews(@RequestBody RichForm richForm) {
      richForm.validate();
      logger.info("request info : {}", richForm);
      return Collections.emptyList();
    }

    @ApiOperation(value = "获取当前信息", notes = "", response = CurrentInfoVO.class)
    @PostMapping("/current_info")
    public CurrentInfoVO getCurrentInfo(){
        CurrentInfoVO currentInfoVO = new CurrentInfoVO(0, 0, 0, 100);
        return currentInfoVO;
    }

    @ApiOperation(value = "根据精度获取PVUV统计信息", notes = "", response = PageStatistic.class)
    @PostMapping("/get_statistics")
    public List<PageStatistic> getStatistics(TimeRange timeRange){
        int precision = timeRange.getPrecision();
        if (precision == 1) { //月
            return queryMongo(timeRange, month, ChronoUnit.MONTHS, PageStatistic.class);
        } else if (precision == 3) { // 小时
            return queryMongo(timeRange, hour, ChronoUnit.HOURS, PageStatistic.class);
        } else { // 默认为天
            return queryMongo(timeRange, day, ChronoUnit.DAYS, PageStatistic.class);
        }
    }

    @ApiOperation(value = "根据精度获取不同平台访客数据", notes = "", response = PlatformStatistic.class)
    @PostMapping("/get_platform_statistics")
    public List<PlatformStatistic> getPlatformStatistic(TimeRange timeRange){
        int precision = timeRange.getPrecision();
        if (precision == 1) { // 月
            return queryMongo(timeRange, platformMonths, ChronoUnit.MONTHS, PlatformStatistic.class);
        } else if (precision == 3) { // 小时
            return queryMongo(timeRange, platformHours, ChronoUnit.HOURS, PlatformStatistic.class);
        } else { // 默认为天
            return queryMongo(timeRange, platformDays, ChronoUnit.DAYS, PlatformStatistic.class);
        }
    }

    @ApiOperation(value = "根据精度获取不同认证状态用户数据", notes = "", response = AuthStatistic.class)
    @PostMapping("/get_auth_statistic")
    public List<AuthStatistic> getAuthStatistic(TimeRange timeRange){
        int precision = timeRange.getPrecision();
        if (precision == 1) { // 月
            return queryMongo(timeRange, authMonths, ChronoUnit.MONTHS, AuthStatistic.class);
        } else if (precision == 3) { // 小时
            return queryMongo(timeRange, authHours, ChronoUnit.HOURS, AuthStatistic.class);
        } else { // 默认为天
            return queryMongo(timeRange, authDays, ChronoUnit.DAYS, AuthStatistic.class);
        }
    }
}
