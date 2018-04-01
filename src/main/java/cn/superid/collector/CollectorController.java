package cn.superid.collector;

import cn.superid.collector.entity.PageView;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  public CollectorController(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @PostMapping("/page")
  public void queryFile(@RequestBody PageView pageView, HttpServletRequest request) {
    pageView.setEpoch(Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
    pageView.setIp(request.getRemoteAddr());
    sendMessage("collector.page", pageView.toString());
  }

  private void sendMessage(String topicName, String msg) {
    kafkaTemplate.send(topicName, msg);
  }

}
