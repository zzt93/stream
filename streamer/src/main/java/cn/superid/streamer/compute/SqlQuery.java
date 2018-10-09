package cn.superid.streamer.compute;


import cn.superid.collector.entity.view.PageView;
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

  @Autowired
  public SqlQuery(SparkSession spark, @Value("${collector.mongo.page}")String pages) {
    //把mongodb中的数据加载到spark中
    pageView = MongoSpark.load(spark, MongoConfig.readConfig(spark, pages), PageView.class);
    //创建spark的临时视图，用于查询
    pageView.createOrReplaceTempView(pages);
  }

  /**
   * 从spark中查询mongodb中的内容
   * @param query
   * @return
   */
  public String query(String query) {
    Dataset<Row> sql = pageView.sqlContext().sql(query);
    sql.show();
    return "";
  }
}
