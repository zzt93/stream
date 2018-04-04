package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import javax.servlet.http.HttpServletRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@RestController
@RequestMapping("/collector")
public class CollectorController {

  private static final Logger logger = LoggerFactory.getLogger(CollectorController.class);
  private static final String DIR = "hdfs://192.168.1.204:14000/collector/page/";
  @Value("${collector.buffer.size}")
  private int size = 1000;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final SparkSession spark;
  private final ThreadLocal<List<PageView>> lists = ThreadLocal.withInitial(LinkedList::new);

  @Autowired
  public CollectorController(KafkaTemplate<String, String> kafkaTemplate,
      SparkSession spark) {
    this.kafkaTemplate = kafkaTemplate;
    this.spark = spark;
  }

  @PostMapping("/page")
  public void queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
    pageView.setEpoch(Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
    pageView.setIp(request.getRemoteAddr());
    save(spark, pageView);
    sendMessage("collector.page", pageView.toString());
  }

  private void save(SparkSession spark, PageView pageView) {
    lists.get().add(pageView);
    if (lists.get().size() >= size) {
      Dataset<Row> ds = spark
          .createDataFrame(lists.get(), PageView.class);
      lists.set(new LinkedList<>());
      ds.write().parquet(DIR);
    }
  }

  private void sendMessage(String topicName, String msg) {
    kafkaTemplate.send(topicName, msg);
  }

}
