package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@RestController
@RequestMapping("/collector")
public class CollectorController {

  private static final Logger logger = LoggerFactory.getLogger(CollectorController.class);
   static final String DIR = "hdfs://192.168.1.204:14000/collector/page/";
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final SparkSession spark;
  private final SqlQuery sqlQuery;
  /**
   * estimatedSize <= queue.size()
   */
  private ConcurrentLinkedDeque<PageView> queue = new ConcurrentLinkedDeque<>();
  private AtomicLong estimatedSize = new AtomicLong();
  @Value("${collector.buffer.size}")
  private int size = 1000;

  @Autowired
  public CollectorController(KafkaTemplate<String, String> kafkaTemplate,
      SparkSession spark, SqlQuery sqlQuery) {
    this.kafkaTemplate = kafkaTemplate;
    this.spark = spark;
    this.sqlQuery = sqlQuery;
  }

  @CrossOrigin(origins = "*")
  @PostMapping("/page")
  public void queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
//    if (pageView.getFrontVersion() == null) return;
    pageView.setEpoch(Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
    pageView.setIp(request.getRemoteAddr());
    pageView.setDevice(request.getHeader("User-Agent"));
    pageView.setHost(request.getHeader("Host"));
    save(pageView);
    sendMessage("collector.page", pageView);
  }

  @GetMapping("/flush")
  public void flush() {
    ConcurrentLinkedDeque<PageView> tmp = this.queue;
    this.queue = new ConcurrentLinkedDeque<>();
    estimatedSize.set(queue.size());

    Dataset<Row> ds = spark.createDataFrame(new LinkedList<>(tmp), PageView.class);
    ds.write().mode(SaveMode.Append).parquet(DIR);
  }

  @GetMapping("/peek")
  public String peek(@RequestParam boolean tail) {
    StringBuilder sb = new StringBuilder(2000);
    sb.append("[").append(estimatedSize.get()).append("]");
    Iterator<PageView> it;
    if (tail) {
      it = queue.descendingIterator();
    } else {
      it = queue.iterator();
    }
    for (int i = 0; i < Math.min(20, estimatedSize.get()) && it.hasNext(); i++) {
      sb.append(it.next());
    }
    return sb.toString();
  }

  @PostMapping("/query")
  public String query(@RequestBody String query) {
    return sqlQuery.query(query);
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
