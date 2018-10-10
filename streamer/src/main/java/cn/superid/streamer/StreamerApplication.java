package cn.superid.streamer;

import cn.superid.streamer.compute.StructuredStreamQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 流计算程序，从kafka的"collector.page"topic中读取页面浏览信息，计算完了之后存入kafka的"streamer.minute"topic，
 * 然后由MinuteWriter类消费kafka中的消息，保存到mongodb。
 * 当前端调用StreamerController中的接口时，去mongodb中查询。
 *
 * RegularQuery类会定时从mongodb中查询数据计算pv uv 信息，然后保存到相应的mongodb集合中
 */
@SpringBootApplication
@EnableScheduling
@EnableKafka
public class StreamerApplication implements CommandLineRunner {

  private final StructuredStreamQuery streamQuery;

  @Autowired
  public StreamerApplication(StructuredStreamQuery streamQuery) {
    this.streamQuery = streamQuery;
  }

  public static void main(String[] args) {
    SpringApplication.run(StreamerApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    streamQuery.run();
  }
}
