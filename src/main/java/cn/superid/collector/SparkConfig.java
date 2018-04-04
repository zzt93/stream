package cn.superid.collector;

import org.apache.spark.SparkConf;
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
//        .set("fs.alluxio.impl", "alluxio.client.file.BaseFileSystem")
//        .set("spark.driver.extraClassPath", "/<PATH_TO_ALLUXIO>/client/alluxio-1.8.0-SNAPSHOT-client.jar")
//        .set("spark.executor.extraClassPath", "/<PATH_TO_ALLUXIO>/client/alluxio-1.8.0-SNAPSHOT-client.jar")
        ;
  }

}
