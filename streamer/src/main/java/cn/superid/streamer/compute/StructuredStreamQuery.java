package cn.superid.streamer.compute;

import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

import cn.superid.collector.entity.PageView;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.api.java.function.MapFunction;
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
public class StructuredStreamQuery implements Serializable {

  private final SparkSession spark;
  private final String servers;

  @Autowired
  public StructuredStreamQuery(KafkaProperties kafkaProperties, SparkSession spark) {
    this.spark = spark;
    servers = kafkaProperties.getBootstrapServers().stream().collect(
        Collectors.joining(","));
  }

  public void run() throws StreamingQueryException {
    Dataset<Row> df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", "collector.page")
        .option(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            JsonDeserializer.class.getCanonicalName())
        .load();

    Dataset<PageView> views = df.map(
        (MapFunction<Row, PageView>) value -> PageView
            .fromString(new String((byte[]) value.get(1))), Encoders.bean(PageView.class));

    Dataset<Row> pageCounts = views
        .withWatermark("epoch", "1 minute")
        .groupBy(functions.window(col("epoch"), "30 minutes", "1 minute"))
        .agg(count(col("*")).as("pv"));
    Dataset<Row> idCounts = views.select(col("viewId"), col("epoch"))
        .withWatermark("epoch", "1 minute")
        .groupBy(functions.window(col("epoch"), "30 minutes", "1 minute"))
        .agg(approx_count_distinct("viewId").alias("uv"));
    Dataset<Row> userCount = views.select(col("userId"), col("epoch"))
        .withWatermark("epoch", "1 minute")
        .groupBy(functions.window(col("epoch"), "30 minutes", "1 minute"))
        .agg(approx_count_distinct("userId").alias("uv (signed user)"));

    for (StreamingQuery query : getStreamingQuery(pageCounts, idCounts, userCount)) {
      query.awaitTermination();
    }
  }

  @SafeVarargs
  private final StreamingQuery[] getStreamingQuery(Dataset<Row>... datasets) {
    StreamingQuery[] res = new StreamingQuery[datasets.length];
    for (int i = 0; i < datasets.length; i++) {
      res[i] = datasets[i]
          .writeStream()
//          .foreach(new StreamQueryWriter())
          .outputMode("update")
          .format("console")
          .option("truncate", false)
          .option("numRows", 24)
          .start();
    }
    return res;
  }


}
