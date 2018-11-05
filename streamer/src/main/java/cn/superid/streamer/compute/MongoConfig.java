package cn.superid.streamer.compute;

import com.mongodb.spark.config.ReadConfig;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * @author zzt
 */
@Configuration
public class MongoConfig {

  private static final int SIZE_OF_PAGE_VIEW = 500;

  /**
   * 如果mongodb的指定集合（类似传统数据库中的database的概念）不存在的话，就创建
   * @param mongo
   * @param collection
   * @param max
   */
  public static void createIfNotExist(MongoTemplate mongo, String collection, int max) {
    if (mongo.collectionExists(collection)) {
      return;
    }
    CollectionOptions options = CollectionOptions.empty().capped().maxDocuments(max)
        .size(max * SIZE_OF_PAGE_VIEW);
    mongo.createCollection(collection, options);
  }

  @Bean
  public MappingMongoConverter mappingMongoConverter(MongoDbFactory factory,
      MongoMappingContext context) {
    DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
    MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, context);
    mappingConverter.setCustomConversions(customConversions());
    mappingConverter.setTypeMapper(new DefaultMongoTypeMapper(null));//去掉默认mapper添加的_class
    return mappingConverter;
  }

  private CustomConversions customConversions() {
    List<Converter> list = new ArrayList<>();
    list.add(new TimestampConverter());
    return new MongoCustomConversions(list);
  }

  public static ReadConfig readConfig(SparkSession sparkSession, String collection) {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.mongodb.input.collection", collection);
    return ReadConfig.create(sparkSession).withOptions(overrides);
  }

  class TimestampConverter implements Converter<Date, Timestamp> {

    @Override
    public Timestamp convert(Date date) {
      return new Timestamp(date.getTime());
    }
  }
}
