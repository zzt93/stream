package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import cn.superid.collector.entity.SimpleResponse;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
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
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final MongoTemplate mongo;

  private final ConcurrentLinkedDeque<PageView> queue = new ConcurrentLinkedDeque<>();
  private final AtomicLong estimatedSize = new AtomicLong();
  @Value("${collector.mongo.page}")
  private String pages;
  @Autowired
  public CollectorController(KafkaTemplate<String, String> kafkaTemplate, MongoTemplate mongo) {
    this.kafkaTemplate = kafkaTemplate;
    this.mongo = mongo;
  }

  @CrossOrigin(origins = "*")
  @PostMapping("/page")
  public SimpleResponse queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
//    if (pageView.getFrontVersion() == null) return;
    pageView.setEpoch(Timestamp.valueOf(LocalDateTime.now()));
    pageView.setClientIp(request.getRemoteAddr());
    pageView.setDevice(request.getHeader("User-Agent"));
    pageView.setServerIp(request.getHeader("Host"));
    pageView.setDomain(request.getHeader("x-original"));
    save(pageView);
    sendMessage("collector.page", pageView);
    return new SimpleResponse(0);
  }

  @GetMapping("/peek")
  public String peek() {
    StringBuilder sb = new StringBuilder(2000);
    sb.append("[").append(estimatedSize.get()).append("]");
    Iterator<PageView> it = queue.iterator();
    for (int i = 0; i < Math.min(20, estimatedSize.get()) && it.hasNext(); i++) {
      sb.append(it.next());
    }
    return sb.toString();
  }

  @Async
  void save(PageView pageView) {
    try {
      mongo.insert(pageView, pages);
    } catch (Exception e) {
      logger.error("", e);
    }

    queue.addLast(pageView);
    if (queue.size() == 20) queue.removeFirst();
    estimatedSize.incrementAndGet();
  }

  private void sendMessage(String topicName, PageView msg) {
    HashMap<String, Object> map = new HashMap<>();
    map.put(KafkaHeaders.TOPIC, topicName);
    kafkaTemplate.send(new GenericMessage<>(msg.toString(), map));
  }

}
