package cn.superid.collector;

import java.io.Serializable;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

/**
 * @author zzt
 */
@Service
public class TimeTriggerQuery implements Serializable {

  private final SparkSession spark;
  private final String servers;

  @Autowired
  public TimeTriggerQuery(KafkaProperties kafkaProperties, SparkSession spark) {
    this.spark = spark;
    servers = kafkaProperties.getBootstrapServers().stream().collect(
        Collectors.joining(","));
  }

  void run() {
    Dataset<Row> df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", "collector.page")
        .option(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            JsonDeserializer.class.getCanonicalName())
        .load();
    df.writeStream()
        .format("console")
        .trigger(Trigger.ProcessingTime("24 hours"))
        .start();
  }
}
