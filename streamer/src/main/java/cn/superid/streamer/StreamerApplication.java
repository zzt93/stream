package cn.superid.streamer;

import cn.superid.streamer.compute.StructuredStreamQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
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
