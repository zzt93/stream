package cn.superid.streamer.compute;

import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

import cn.superid.collector.entity.view.PageView;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;


/**
 * 从kafka中读取页面浏览信息，计算pv uv 后保存到kafka的另外的topic中
 *
 * @author zzt
 */
@Service
public class StructuredStreamQuery implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredStreamQuery.class);

    private final SparkSession spark;
    private final String kafkaBrokers;
    private final String streamerTopic;
    private final String streamerRichTopic;
    private final String hdfsCheckpoint;
    private final String richHdfsCheckpoint;

    @Autowired
    public StructuredStreamQuery(KafkaProperties kafkaProperties, SparkSession spark,
                                 @Value("${streamer.kafka.minute}") String streamerTopic,
                                 @Value("${streamer.kafka.minute.rich}") String streamerRichTopic,
                                 @Value("${streamer.hdfs.minute}") String hdfsCheckpoint,
                                 @Value("${streamer.hdfs.rich.minute}") String richHdfsCheckpoint) {
        this.spark = spark;
        this.kafkaBrokers = kafkaProperties.getBootstrapServers().stream().collect(
                Collectors.joining(","));
        this.streamerTopic = streamerTopic;
        this.streamerRichTopic = streamerRichTopic;
        this.hdfsCheckpoint = hdfsCheckpoint;
        this.richHdfsCheckpoint = richHdfsCheckpoint;
    }


    /**
     * 统计pv uv
     *
     * @throws StreamingQueryException
     */
    public void run() throws StreamingQueryException {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                //消费页面浏览信息
                .option("subscribe", "collector.page")
                .option("enable.auto.commit", true)
                .option("auto.offset.reset", "latest")
                .option("failOnDataLoss", false)
                .load();

        //df.map需要两个参数，一个是MapFunction，一个是Encoder
        Dataset<PageView> views = df.map(
                new MapFunction<Row, PageView>() {
                    @Override
                    public PageView call(Row value) throws Exception {
                        return PageView.fromString(new String((byte[]) value.get(1)));
                    }
                }, Encoders.bean(PageView.class));

        minutePvAndUv(views);

    }

    /**
     * 每分钟，按照更多维度计算pv uv
     *
     * @param views
     */
    private void richMinutePvAndUv(Dataset<PageView> views) {
        //按照时间、设备、盟id、事务id、目标id、是否是公网ip维度统计pv uv
        Dataset<String> richPvAndUv = views
//        .withWatermark("epoch", "1 minute")
                .groupBy(functions.window(col("epoch"), "1 minute", "1 minute").as("epoch"),
                        views.col("deviceType"),
                        views.col("allianceId"),
                        views.col("affairId"),
                        views.col("targetId"),
                        views.col("publicIp")
                )
                .agg(count(col("*")).as("pv"), approx_count_distinct("viewId").alias("uv"),
                        approx_count_distinct("userId").alias("uvSigned"))
                .withColumn("epoch", col("epoch.end"))
                .toJSON().as("value");

        new Thread(new StructuredStreamQueryRunner(kafkaBrokers, streamerRichTopic, richHdfsCheckpoint, richPvAndUv)).run();
    }

    /**
     * 每分钟，按照时间维度计算pv uv
     *
     * @param views
     */
    private void minutePvAndUv(Dataset<PageView> views) {
        //只按照时间维度分组统计pv uv
        Dataset<String> pageCounts = views
//        .withWatermark("epoch", "1 minute")
                .groupBy(functions.window(col("epoch"), "1 minute", "1 minute").as("epoch"))
                .agg(count(col("*")).as("pv"), approx_count_distinct("viewId").alias("uv"),
                        //uvSigned是登陆用户数
                        approx_count_distinct("userId").alias("uvSigned"))
                .withColumn("epoch", col("epoch.end"))
                .toJSON().as("value");

        new Thread(new StructuredStreamQueryRunner(kafkaBrokers, streamerTopic, hdfsCheckpoint, pageCounts)).run();
    }


}
