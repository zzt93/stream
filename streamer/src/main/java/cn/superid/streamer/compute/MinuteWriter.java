package cn.superid.streamer.compute;

import cn.superid.collector.entity.view.PageStatistic;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * spark计算好的pv uv 等信息放到kafka的"streamer.minute"topic中
 * 该类消费"streamer.minute"topic，然后写入mongodb的"minutes"集合，给前端页面调用StreamerController的时候查询
 * @author zzt
 */
@Service
public class MinuteWriter {

  private static final Logger logger = LoggerFactory.getLogger(MinuteWriter.class);
  private static final Gson gson = new Gson();
  private final MongoTemplate mongo;
  @Value("${collector.mongo.minute}")
  private String minute;

  @Autowired
  public MinuteWriter(MongoTemplate mongo) {
    this.mongo = mongo;
  }


  @KafkaListener(topics = "${streamer.kafka.minute}")
  public void listen(String message) {
    logger.debug("Received message in group:  {}", message);
    PageStatistic pageStatistic = gson.fromJson(message, PageStatistic.class);
    pageStatistic.setId(pageStatistic.getEpoch().getTime());
    mongo.save(pageStatistic, minute);
  }
}
