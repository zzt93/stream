package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import java.util.Collections;
import java.util.stream.Collectors;
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
import org.springframework.stereotype.Service;

/**
 * @author zzt
 */
@Service
public class Structured {

  private final KafkaProperties kafkaProperties;

  @Autowired
  public Structured(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  void run() throws StreamingQueryException {
    SparkSession spark = SparkSession
        .builder()
        .master("local")
        .appName("Windowed count")
        .getOrCreate();

    String servers = kafkaProperties.getBootstrapServers().stream().collect(
        Collectors.joining(","));
    Dataset<Row> df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", "collector.page")
        .load();

    Dataset<PageView> views = df.flatMap(
        (FlatMapFunction<Row, PageView>) x -> Collections
            .singletonList(new PageView(new String((byte[]) x.get(1)).split(" "))).iterator(),
        Encoders.bean(PageView.class));

    Dataset<Row> pageCounts = views.groupBy(
        functions.window(views.col("epoch"), "10 minutes", "5 minutes"),
        views.col("pageUri")).count();
    Dataset<Row> idCounts = views
        .groupBy(functions.window(views.col("epoch"), "10 minutes", "5 minutes"),
            views.col("id")).count();

    // Start running the query that prints the running counts to the console
    StreamingQuery pageQuery = pageCounts.writeStream()
        .outputMode("append")
        .format("console")
        .start();
    StreamingQuery idQuery = idCounts.writeStream()
        .outputMode("append")
        .format("console")
        .start();

    pageQuery.awaitTermination();
    idQuery.awaitTermination();
  }

}
