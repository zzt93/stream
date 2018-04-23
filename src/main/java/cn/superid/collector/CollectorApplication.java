package cn.superid.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CollectorApplication {

  public static void main(String[] args) {
    SpringApplication.run(CollectorApplication.class, args);
  }

}
