package cn.superid.streamer.service;

import cn.superid.collector.entity.view.RichPageStatistic;
import cn.superid.streamer.form.RichForm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * @author dufeng
 * @create: 2018-10-15 10:56
 */
@Service
public class StreamerService {
    private final Logger logger = LoggerFactory.getLogger(StreamerService.class);
    /**
     * 限制前端查询时间范围内的时间点个数
     */
    private static final int MINUTES_COUNT_LIMIT = 500;
    private static final int HOURS_COUNT_LIMIT = 500;
    private static final int DAYS_COUNT_LIMIT = 500;

    @Autowired
    private MongoTemplate mongo;

    @Value("${collector.mongo.day.rich}")
    private String dayRich;
    @Value("${collector.mongo.hour.rich}")
    private String hourRich;
    @Value("${collector.mongo.minute.rich}")
    private String minuteRich;

    /**
     * 以分钟为单位获取pv uv信息
     *
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInMinutes(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        if (toLocalDateTime.isAfter(LocalDateTime.now())) {
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES);
        }

        long minutesCount = Duration.between(fromLocalDateTime, toLocalDateTime).toMinutes();
        if ( minutesCount > MINUTES_COUNT_LIMIT) {
            System.out.println("查询时间范围内包含的分钟时间点过多:"+minutesCount);
            return Collections.emptyList();
        }

        return getRichPageStatistics(fromLocalDateTime, toLocalDateTime, richForm, minuteRich);
    }


    /**
     * 以小时为单位获取pv uv信息
     *
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInHours(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.HOURS);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.HOURS);
        if (toLocalDateTime.isAfter(LocalDateTime.now())) {
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS);
        }

        long hoursCount = Duration.between(fromLocalDateTime, toLocalDateTime).toHours();
        if (hoursCount > HOURS_COUNT_LIMIT) {
            System.out.println("查询时间范围内包含的小时时间点过多:" + hoursCount);
            return Collections.emptyList();
        }

        return getRichPageStatistics(fromLocalDateTime, toLocalDateTime, richForm, hourRich);
    }

    /**
     * 以天为单位获取pv uv信息
     *
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInDays(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
        if (toLocalDateTime.isAfter(LocalDateTime.now())) {
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS);
        }

        long daysCount = Duration.between(fromLocalDateTime, toLocalDateTime).toDays();
        if (daysCount > DAYS_COUNT_LIMIT) {
            System.out.println("查询时间范围内包含的天时间点过多:"+daysCount);
            return Collections.emptyList();
        }

        return getRichPageStatistics(fromLocalDateTime, toLocalDateTime, richForm, dayRich);
    }


    private List<RichPageStatistic> getRichPageStatistics(LocalDateTime fromLocalDateTime, LocalDateTime toLocalDateTime,
                                                          RichForm richForm, String collectionName) {

        //把前端传过来的条件放到列表中，然后转换成一个变长数组传递给andOperator方法
        List<Criteria> criteriaList = new ArrayList<>();
        criteriaList.add(Criteria.where("epoch").gt(Timestamp.valueOf(fromLocalDateTime)));
        criteriaList.add(Criteria.where("epoch").lte(Timestamp.valueOf(toLocalDateTime)));
        if(richForm.getAffairId()>0){
            criteriaList.add(Criteria.where("affairId").is(richForm.getAffairId()));
        }
        if(richForm.getTargetId()>0){
            criteriaList.add(Criteria.where("targetId").is(richForm.getTargetId()));
        }
        if (!StringUtils.isEmpty(richForm.getDevType())) {
            criteriaList.add(Criteria.where("deviceType").is(richForm.getDevType()));
        }

        Criteria[] criterias = new Criteria[criteriaList.size()];
        for(int i = 0 ;i < criterias.length;i++){
            criterias[i] = criteriaList.get(i);
        }
        Criteria timeCriteria = new Criteria().andOperator(criterias);
        //查询好像不可以有多个andOperator，所以把多个Criteria放进一个数组中传给一个andOperator

        Criteria ipCriteria = Criteria.where("publicIp").is(true);
        Query query = Query.query(new Criteria().andOperator(timeCriteria, ipCriteria)).with(Sort.by(Sort.Direction.ASC, "epoch"));

        LinkedList<RichPageStatistic> pageStatistics = new LinkedList<>(mongo.find(query, RichPageStatistic.class, collectionName));

//        ListIterator<RichPageStatistic> it = pageStatistics.listIterator();
//        for (int i = pageStatistics.size() - 1; i >= 0; i--) {
//            LocalDateTime time = null;
//            if("minute".equals(richForm.getTimeUnit())){
//                time = toLocalDateTime.minusMinutes(i);
//            } else if ("hour".equals(richForm.getTimeUnit())) {
//                time = toLocalDateTime.minusHours(i);
//            } else if ("day".equals(richForm.getTimeUnit())) {
//                time = toLocalDateTime.minusDays(i);
//            }
//
//            boolean hasMore = it.hasNext();
//            if (hasMore && time.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli() == it.next().getId()) {
//            } else {
//                if (it.hasPrevious() && hasMore) {
//                    it.previous();
//                }
//                RichPageStatistic pageStatistic = new RichPageStatistic(Timestamp.valueOf(time));
//                it.add(pageStatistic);
//            }
//        }

        return pageStatistics;
    }
}
