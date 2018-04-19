package cn.superid.collector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CollectorApplication implements CommandLineRunner {

  private final StreamQuery streamQuery;
  private final CollectorController controller;

  @Autowired
  public CollectorApplication(StreamQuery streamQuery, CollectorController controller) {
    this.streamQuery = streamQuery;
    this.controller = controller;
  }

  public static void main(String[] args) {
    SpringApplication.run(CollectorApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    Runtime.getRuntime().addShutdownHook(new Thread(new FlushHook(controller)));
    streamQuery.run();
  }
}
