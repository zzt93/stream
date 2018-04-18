package cn.superid.collector;

import static cn.superid.collector.CollectorController.DIR;

import cn.superid.collector.entity.PageView;
import java.sql.Timestamp;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author zzt
 */
@Service
public class SqlQuery {

  private final Dataset<PageView> pages;

  @Autowired
  public SqlQuery(SparkSession spark) {
    pages = spark.read().parquet(DIR).map(
        (MapFunction<Row, PageView>) value ->
            new PageView.PageBuilder()
                .setId(value.getString(3))
                .setUserId(value.getString(6))
                .setPageUri(value.getString(5))
                .setTimestamp((Timestamp) value.get(1))
                .setDevice(value.getString(0))
                .setServerIp(value.getString(2))
                .setClientIp(value.getString(4))
                .build(),
        Encoders.bean(PageView.class));
    pages.createOrReplaceTempView("pages");
  }

  public String query(@RequestBody String query) {
    Dataset<Row> sql = pages.sqlContext().sql(query);
    sql.show();
    return "";
  }
}
