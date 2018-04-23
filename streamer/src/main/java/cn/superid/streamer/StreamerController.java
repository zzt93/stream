package cn.superid.streamer;

import cn.superid.collector.entity.PageStatistic;
import cn.superid.streamer.compute.SqlQuery;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzt
 */
@RestController
@RequestMapping("/streamer")
public class StreamerController {

  private static final Logger logger = LoggerFactory.getLogger(StreamerController.class);
  private final SqlQuery sqlQuery;
  private final MongoTemplate mongo;
  @Value("${collector.mongo.page}")
  private String pages;

  @Autowired
  public StreamerController(SqlQuery sqlQuery, MongoTemplate mongo) {
    this.sqlQuery = sqlQuery;
    this.mongo = mongo;
  }

  @PostMapping("/query")
  public String query(@RequestBody String query) {
    return sqlQuery.query(query);
  }

  @PostMapping("/hour")
  public List<PageStatistic> statistics() {
    return null;
  }

}
