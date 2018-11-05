package cn.superid.streamer.compute;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

/**
 * @author dufeng
 * @create: 2018-10-10 16:24
 */
public class StructuredStreamQueryRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RegularQuery.class);

    private String kafkaBootstrapServers;
    /**
     * kafka topic
     */
    private String kafkaTopic;
    /**
     * hdfs上的检查点，用于记录状态
     */
    private String hdfsCheckpoint;
    /**
     * Spark数据集
     */
    private Dataset<String> dataset;

    public StructuredStreamQueryRunner(String kafkaBootstrapServers,String kafkaTopic, String hdfsCheckpoint, Dataset<String> dataset) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.hdfsCheckpoint = hdfsCheckpoint;
        this.dataset = dataset;
    }

    @Override
    public void run() {
        logger.info("output kafka topic :" + kafkaTopic);

            dataset.writeStream()
                    .outputMode("update")
                    .format("kafka")
                    .option("kafka.bootstrap.servers", this.kafkaBootstrapServers)
                    .option("topic", this.kafkaTopic)
                    .option("checkpointLocation", this.hdfsCheckpoint)
                    .trigger(ProcessingTime("20 seconds"))
//          .format("console")
//          .option("truncate", false)
//          .option("numRows", 50)
                    .start();
//控制台输出内容
//            dataset.writeStream()
//                    .outputMode("update")
//                    .format("console")
//                    //truncate设置为false，控制台输出的才是完整的
//                    .option("truncate", false)
//                    .trigger(ProcessingTime("20 seconds"))
//                    .start();
    }
}
