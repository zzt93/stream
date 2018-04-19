package cn.superid.collector;

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

/**
 * @author zzt
 */
public class StreamQueryWriter extends ForeachWriter<Row> implements Serializable {

  private JavaHBaseContext hbaseContext;

  @Override
  public boolean open(long version, long partition) {
    Configuration conf = HBaseConfiguration.create();
    hbaseContext = new JavaHBaseContext(new JavaSparkContext(), conf);

    return false;
  }

  @Override
  public void process(Row row) {
//    hbaseContext.bulkPut(,
//        TableName.valueOf(""),
//        new PutFunction());
  }

  @Override
  public void close(Throwable throwable) {

  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] part = v.split(",");
      Put put = new Put(Bytes.toBytes(part[0]));

      put.addColumn(Bytes.toBytes(part[1]),
          Bytes.toBytes(part[2]),
          Bytes.toBytes(part[3]));
      return put;
    }

  }
}
