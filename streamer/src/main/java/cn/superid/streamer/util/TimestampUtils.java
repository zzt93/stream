package cn.superid.streamer.util;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * @author zzt
 */
public class TimestampUtils {

  public static Timestamp truncate(Timestamp timestamp, ChronoUnit unit) {
    return Timestamp.valueOf(timestamp.toLocalDateTime().truncatedTo(unit));
  }
}
