package cn.superid.streamer.compute;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
@Configuration
public class SparkConfig {

    @Value("${collector.spark.app.name:streamer}")
    private String appName;
    @Value("${collector.spark.master.uri:local}")
    private String masterUri;
    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;
    @Value("${collector.mongo.minute}")
    private String minute;

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
                .set("spark.driver.port", "40000")
                .setJars(new String[]{"/app.jar"})
//        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                ;
    }

    @Bean
    public MongoForeachWriter writer(MongoProperties properties) {
        return new MongoForeachWriter(properties.getUri(), minute);
    }

}
