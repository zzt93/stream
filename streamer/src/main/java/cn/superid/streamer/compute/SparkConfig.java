package cn.superid.streamer.compute;

import com.mongodb.spark.config.ReadConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
public class SparkConfig {

  @Value("${collector.spark.app.name:collector}")
  private String appName;
  @Value("${collector.spark.master.uri:local}")
  private String masterUri;
  @Value("${spring.data.mongodb.uri}")
  private String mongoUri;

  static ReadConfig readConfig(SparkSession sparkSession, String collection) {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.mongodb.input.collection", collection);
    return ReadConfig.create(sparkSession).withOptions(overrides);
  }

  @Bean
  public SparkSession sparkSession(SparkConf conf) {
    return SparkSession
        .builder()
        .config(conf)
        .getOrCreate();
  }

  @Bean
  public SparkConf sparkConf() {
    return new SparkConf()
        .setAppName(appName)
        .setMaster(masterUri)
        .set("spark.mongodb.input.uri", mongoUri)
        .set("spark.mongodb.output.uri", mongoUri)
        .setJars(new String[]{"out/artifacts/collector_jar/collector.jar"})
//        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        ;
  }

}
