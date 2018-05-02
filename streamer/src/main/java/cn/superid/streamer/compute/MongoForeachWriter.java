package cn.superid.streamer.compute;

import cn.superid.collector.entity.PageStatistic;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.io.Serializable;
import org.apache.spark.sql.ForeachWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @author zzt
 */
public class MongoForeachWriter extends ForeachWriter<PageStatistic> implements Serializable {

  private final String uri;
  private final String minute;
  private MongoTemplate template;

  public MongoForeachWriter(String uri, String minute) {
    this.uri = uri;
    this.minute = minute;
  }

  @Override
  public boolean open(long version, long partition) {
    MongoClient mongo = new MongoClient(new MongoClientURI(uri));
    template = new MongoTemplate(mongo, minute);
    return true;
  }

  @Override
  public void process(PageStatistic pageStatistic) {
    template.insert(pageStatistic);
  }

  @Override
  public void close(Throwable throwable) {
  }
}
