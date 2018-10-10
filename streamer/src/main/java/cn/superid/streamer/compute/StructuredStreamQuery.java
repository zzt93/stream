package cn.superid.streamer.compute;

import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

import cn.superid.collector.entity.view.PageView;

import java.io.Serializable;
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
 * @author zzt
 */
@Service
public class StructuredStreamQuery implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredStreamQuery.class);

    private final SparkSession spark;
    private final String servers;
    private final String streamerTopic;
    private final String hdfsCheckpoint;

    @Autowired
    public StructuredStreamQuery(KafkaProperties kafkaProperties, SparkSession spark,
                                 @Value("${streamer.kafka.minute}") String streamerTopic,
                                 @Value("${streamer.hdfs.minute}") String hdfsCheckpoint) {
        this.spark = spark;
        servers = kafkaProperties.getBootstrapServers().stream().collect(
                Collectors.joining(","));
        this.streamerTopic = streamerTopic;
        this.hdfsCheckpoint = hdfsCheckpoint;
    }

    public void run() throws StreamingQueryException {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", servers)
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

        Dataset<String> pageCounts = views
//        .withWatermark("epoch", "1 minute")
                .groupBy(functions.window(col("epoch"), "1 minute", "1 minute").as("epoch"))
                .agg(count(col("*")).as("pv"), approx_count_distinct("viewId").alias("uv"),
                        //uvSigned是登陆用户数
                        approx_count_distinct("userId").alias("uvSigned"))
                .withColumn("epoch", col("epoch.end"))
                .toJSON().as("value");

        for (StreamingQuery query : getStreamingQuery(streamerTopic,pageCounts)) {
            query.awaitTermination();
        }

        Dataset<String> richPvAndUv = views
//        .withWatermark("epoch", "1 minute")
                .groupBy(functions.window(col("epoch"), "1 minute", "1 minute").as("epoch"),
                        col("deviceType"),
                        col("allianceId"),
                        col("affairId"),
                        col("targetId")
                )
                .agg(count(col("*")).as("pv"), approx_count_distinct("viewId").alias("uv"),
                        approx_count_distinct("userId").alias("uvSigned"))
                .withColumn("epoch", col("epoch.end"))
                .toJSON().as("value");
        System.out.println("richPvAndUv="+richPvAndUv.collect());

        for (StreamingQuery query : getStreamingQuery("rich_pv_uv",richPvAndUv)) {
            query.awaitTermination();
        }
    }

    @SafeVarargs
    private final StreamingQuery[] getStreamingQuery(String kafkaTopic,Dataset<String>... datasets) {
        StreamingQuery[] res = new StreamingQuery[datasets.length];
        for (int i = 0; i < datasets.length; i++) {
            res[i] = datasets[i]
                    .writeStream()
                    .outputMode("update")
                    .format("kafka")
                    .option("kafka.bootstrap.servers", servers)
                    .option("topic", kafkaTopic)
                    .option("checkpointLocation", hdfsCheckpoint)
                    .trigger(ProcessingTime("20 seconds"))
//          .format("console")
//          .option("truncate", false)
//          .option("numRows", 50)
                    .start();
        }
        return res;
    }

}
