package cn.superid.collector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CollectorApplication implements CommandLineRunner {

  private final Structured structured;
  private final CollectorController controller;

  @Autowired
  public CollectorApplication(Structured structured, CollectorController controller) {
    this.structured = structured;
    this.controller = controller;
  }

  public static void main(String[] args) {
    SpringApplication.run(CollectorApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    Runtime.getRuntime().addShutdownHook(new Thread(new FlushHook(controller)));
    structured.run();
  }
}
