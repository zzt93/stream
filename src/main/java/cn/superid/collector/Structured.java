package cn.superid.collector;

import static org.apache.spark.sql.functions.col;

import cn.superid.collector.entity.PageView;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

/**
 * @author zzt
 */
@Service
public class Structured {

  private final KafkaProperties kafkaProperties;
  private final SparkSession spark;

  @Autowired
  public Structured(KafkaProperties kafkaProperties, SparkSession spark) {
    this.kafkaProperties = kafkaProperties;
    this.spark = spark;
  }

  void run() throws StreamingQueryException {
    String servers = kafkaProperties.getBootstrapServers().stream().collect(
        Collectors.joining(","));
    Dataset<Row> df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", "collector.page")
        .option(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            JsonDeserializer.class.getCanonicalName())
        .load();

    Dataset<PageView> views = df.flatMap(
        (FlatMapFunction<Row, PageView>) x -> Collections
            .singletonList(PageView.fromString(new String((byte[]) x.get(1)))).iterator(),
        Encoders.bean(PageView.class));

    Dataset<Row> pageCounts = views
//        .withWatermark("epoch", "10 minutes")
        .groupBy(
            functions.window(col("epoch"), "1 hour", "5 minutes"),
            col("pageUri")).count();
    Dataset<Row> idCounts = views
//        .withWatermark("epoch", "10 minutes")
        .groupBy(functions.window(col("epoch"), "1 hour", "5 minutes"),
            col("id")).count();

    // Start running the query that prints the running counts to the console
    StreamingQuery pageQuery = pageCounts
        .writeStream()
        .outputMode("complete")
        .format("console")
        .start();
    StreamingQuery idQuery = idCounts
        .writeStream()
        .outputMode("complete")
        .format("console")
        .start();

    pageQuery.awaitTermination();
    idQuery.awaitTermination();
  }

}
