package cn.superid.streamer.util;

import java.sql.Timestamp;
import java.time.ZoneOffset;

/**
 * @author zzt
 */
public class TimestampUtils {

  public static Timestamp addHour(Timestamp timestamp, int hour) {
    return Timestamp.from(timestamp.toLocalDateTime().plusHours(hour).toInstant(ZoneOffset.UTC));
  }

  public static Timestamp addDay(Timestamp timestamp, int day) {
    return Timestamp.from(timestamp.toLocalDateTime().plusDays(day).toInstant(ZoneOffset.UTC));
  }
}
