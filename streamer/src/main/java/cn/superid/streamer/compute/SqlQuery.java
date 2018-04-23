package cn.superid.streamer.compute;


import cn.superid.collector.entity.PageView;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author zzt
 */
@Service
public class SqlQuery {

  private final Dataset<PageView> pageView;
  private final SparkSession spark;
  @Value("${collector.mongo.page}")
  private String pages;

  @Autowired
  public SqlQuery(SparkSession spark) {
    this.spark = spark;
    pageView = MongoSpark.load(spark, SparkConfig.readConfig(spark, this.pages), PageView.class);
    pageView.createOrReplaceTempView("pages");
  }

  public String query(String query) {
    Dataset<Row> sql = pageView.sqlContext().sql(query);
    sql.show();
    return "";
  }
}
