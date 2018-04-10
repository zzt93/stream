package cn.superid.collector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CollectorApplication implements CommandLineRunner {

  @Autowired
  private Structured structured;

  public static void main(String[] args) {
    SpringApplication.run(CollectorApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    structured.run();
  }
}
