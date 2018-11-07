package cn.superid.streamer.service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

import cn.superid.collector.entity.view.PageView;
import cn.superid.collector.entity.view.RichPageStatistic;
import cn.superid.streamer.compute.MongoConfig;
import cn.superid.streamer.compute.Unit;
import cn.superid.streamer.form.RichForm;
import com.mongodb.spark.MongoSpark;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author dufeng
 * @create: 2018-10-15 10:56
 */
@Service
public class StreamerService {

  /**
   * 限制前端查询时间范围内的时间点个数
   */
  private static final int COUNT_LIMIT = 100;
  private final Logger logger = LoggerFactory.getLogger(StreamerService.class);
  private final Dataset<PageView> pageView;

  @Autowired
  public StreamerService(SparkSession spark, @Value("${collector.mongo.page}") String pages) {
    //把mongodb中的数据加载到spark中
    pageView = MongoSpark.load(spark, MongoConfig.readConfig(spark, pages), PageView.class);
    //创建spark的临时视图，用于查询
    pageView.createOrReplaceTempView(pages);
  }

  public List<RichPageStatistic> rangeRichPageviewsInUnit(@RequestBody RichForm richForm,
      Unit unit) {
    LocalDateTime from = unit.truncate(richForm.getFrom().toLocalDateTime());

    long timeDiff = unit.diff(richForm.getFrom(), richForm.getTo());
    if (timeDiff > COUNT_LIMIT) {
      logger.info("查询时间范围内包含的{}过多:{}", unit, timeDiff);
      return Collections.emptyList();
    }

    return getRichPageStatistics(from, (int) timeDiff, unit, richForm);
  }


  private List<RichPageStatistic> getRichPageStatistics(LocalDateTime from, int timeDiff,
      Unit unit, RichForm query) {

    pageView.where(col("publicIp").equalTo(true));

    if (query.getAffairId() != null) {
      pageView.where(col("affairId").equalTo(query.getAffairId()));
      pageView.withColumn("affairId", lit(query.getAffairId()));
    }
    if (query.getTargetId() != null) {
      pageView.where(col("targetId").equalTo(query.getTargetId()));
      pageView.withColumn("targetId", lit(query.getTargetId()));
    }
    if (!StringUtils.isEmpty(query.getDevType())) {
      pageView.where(col("deviceType").equalTo(query.getDevType()));
      pageView.withColumn("deviceType", lit(query.getDevType()));
    }

    Timestamp low = Timestamp.valueOf(from);
    for (int offset = 0; offset < timeDiff; offset++) {
      Timestamp upper = Timestamp.valueOf(unit.update(low, offset + 1));
      pageView.where(col("epoch").between(low, upper));
      Row stat = pageView
          .agg(count("*").as("pv"), countDistinct(col("viewId")).as("uv"),
              countDistinct(col("userId")).as("uvSigned"))
          .withColumn("epoch", lit(upper))
          .first()
          ;
      logger.info("{}", stat);
    }

    return null;
  }
}
