package cn.superid.streamer.compute;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @author dufeng
 * @create: 2018-10-10 17:27
 */
public enum Unit {
  DAY(31, ChronoUnit.DAYS) {
    /**
     * 更新时间操作，加上offset天
     * @param dateTime 被更新的时间
     * @param offset 天数（可以为负数）
     */
    @Override
    public LocalDateTime update(Timestamp dateTime, int offset) {
      return dateTime.toLocalDateTime().plusDays(offset);
    }

    @Override
    public int getUnit(Timestamp timestamp) {
      return timestamp.toLocalDateTime().getDayOfMonth();
    }
    @Override
    public int getDifferenceUnit(Timestamp before, Timestamp after) {
      return (int)Duration.between(after.toLocalDateTime(),before.toLocalDateTime()).toDays();
    }
  }, HOUR(24, ChronoUnit.HOURS) {
    @Override
    public LocalDateTime update(Timestamp dateTime, int offset) {
      return dateTime.toLocalDateTime().plusHours(offset);
    }

    @Override
    public int getUnit(Timestamp timestamp) {
      return timestamp.toLocalDateTime().getHour();
    }
    @Override
    public int getDifferenceUnit(Timestamp before, Timestamp after) {
      return (int)Duration.between(after.toLocalDateTime(),before.toLocalDateTime()).toHours();
    }
  }, MINUTE(30, ChronoUnit.MINUTES) {
    @Override
    public LocalDateTime update(Timestamp dateTime, int offset) {
      return dateTime.toLocalDateTime().plusMinutes(offset);
    }

    @Override
    public int getUnit(Timestamp timestamp) {
      return timestamp.toLocalDateTime().getMinute();
    }

    @Override
    public int getDifferenceUnit(Timestamp before, Timestamp after) {
      return (int)Duration.between(after.toLocalDateTime(),before.toLocalDateTime()).toMinutes();
    }
  };

  public final int range;
  private final ChronoUnit unit;

  Unit(int range, ChronoUnit unit) {
    this.range = range;
    this.unit = unit;
  }

  public int getRange() {
    return range;
  }

  public abstract LocalDateTime update(Timestamp dateTime, int offset);

  public abstract int getUnit(Timestamp timestamp);

  /**
   * 获取两个Timestamp之间的差值
   */
  public abstract int getDifferenceUnit(Timestamp before,Timestamp after);

  public LocalDateTime truncate(LocalDateTime dateTime) {
    return dateTime.truncatedTo(unit);
  }
}
