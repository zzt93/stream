package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServletRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
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
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final SparkSession spark;
  /**
   * estimatedSize <= queue.size()
   */
  private ConcurrentLinkedQueue<PageView> queue = new ConcurrentLinkedQueue<>();
  private AtomicLong estimatedSize = new AtomicLong();
  @Value("${collector.buffer.size}")
  private int size = 1000;

  @Autowired
  public CollectorController(KafkaTemplate<String, String> kafkaTemplate,
      SparkSession spark) {
    this.kafkaTemplate = kafkaTemplate;
    this.spark = spark;
  }

  @CrossOrigin(origins = "*")
  @PostMapping("/page")
  public void queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
    pageView.setEpoch(Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
    pageView.setIp(request.getRemoteAddr());
    pageView.setDevice(request.getHeader("User-Agent"));
    save(pageView);
    sendMessage("collector.page", pageView);
  }

  @GetMapping("/flush")
  public void flush() {
    ConcurrentLinkedQueue<PageView> tmp = this.queue;
    this.queue = new ConcurrentLinkedQueue<>();
    estimatedSize.set(queue.size());

    Dataset<Row> ds = spark.createDataFrame(new LinkedList<>(tmp), PageView.class);
    ds.write().mode(SaveMode.Append).parquet(DIR);
  }

  @Async
  void save(PageView pageView) {
    queue.add(pageView);
    estimatedSize.incrementAndGet();
    if (estimatedSize.updateAndGet(x -> x >= size ? 0 : x) >= size) {
      flush();
    }
  }

  private void sendMessage(String topicName, PageView msg) {
    HashMap<String, Object> map = new HashMap<>();
    map.put(KafkaHeaders.TOPIC, topicName);
    kafkaTemplate.send(new GenericMessage<>(msg.toString(), map));
  }

}
