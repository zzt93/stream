package cn.superid.streamer.service;

import cn.superid.collector.entity.view.RichPageStatistic;
import cn.superid.streamer.form.RichForm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author dufeng
 * @create: 2018-10-15 10:56
 */
@Service
public class StreamerService {
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
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInMinutes(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        if(toLocalDateTime.isAfter(LocalDateTime.now())){
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES);
        }

        if (Duration.between(fromLocalDateTime, toLocalDateTime).toMinutes() > MINUTES_COUNT_LIMIT) {
            throw new RuntimeException("查询时间范围内包含的时间点过多！");
        }

        return getRichPageStatistics(fromLocalDateTime,toLocalDateTime,richForm,minuteRich);
    }


    /**
     * 以小时为单位获取pv uv信息
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInHours(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.HOURS);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.HOURS);
        if(toLocalDateTime.isAfter(LocalDateTime.now())){
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS);
        }

        if (Duration.between(fromLocalDateTime, toLocalDateTime).toHours() > HOURS_COUNT_LIMIT) {
            throw new RuntimeException("查询时间范围内包含的时间点过多！");
        }

        return getRichPageStatistics(fromLocalDateTime,toLocalDateTime,richForm,hourRich);
    }


    /**
     * 以天为单位获取pv uv信息
     * @param richForm
     * @return
     */
    public List<RichPageStatistic> rangeRichPageviewsInDays(@RequestBody RichForm richForm) {
        LocalDateTime fromLocalDateTime = richForm.getFrom().toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
        LocalDateTime toLocalDateTime = richForm.getTo().toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
        if(toLocalDateTime.isAfter(LocalDateTime.now())){
            toLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS);
        }

        if (Duration.between(fromLocalDateTime, toLocalDateTime).toDays() > DAYS_COUNT_LIMIT) {
            throw new RuntimeException("查询时间范围内包含的时间点过多！");
        }

        return getRichPageStatistics(fromLocalDateTime,toLocalDateTime,richForm,dayRich);
    }


    private List<RichPageStatistic> getRichPageStatistics(LocalDateTime fromLocalDateTime, LocalDateTime toLocalDateTime,
                                                          RichForm richForm,String collectionName) {


        Criteria criteria = Criteria.where("epoch")
                .gt(Timestamp.valueOf(fromLocalDateTime))
                .andOperator(Criteria.where("epoch").lte(Timestamp.valueOf(toLocalDateTime))
                        .andOperator(Criteria.where("affairId").is(richForm.getAffairId()))
                        .andOperator(Criteria.where("targetId").is(richForm.getTargetId()))
                        .andOperator(Criteria.where("deviceType").is(richForm.getDevType()))
                        .andOperator(Criteria.where("publicIp").is(true))
                );

        Query query = Query.query(criteria).limit(MINUTES_COUNT_LIMIT).with(Sort.by(Sort.Direction.ASC, "epoch"));

        LinkedList<RichPageStatistic> pageStatistics = new LinkedList<>(mongo.find(query, RichPageStatistic.class, collectionName));

        ListIterator<RichPageStatistic> it = pageStatistics.listIterator();
        for (int i = pageStatistics.size() - 1; i >= 0; i--) {
            LocalDateTime time = toLocalDateTime.minusMinutes(i);
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

        return pageStatistics;
    }
}
