package cn.superid.streamer.compute;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * @author dufeng
 * @create: 2018-10-10 17:27
 */
public enum Unit {
  DAY(31) {
    /**
     * 更新时间操作，加上offset天
     * @param dateTime 被更新的时间
     * @param offset 天数（可以为负数）
     * @return
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
  }, HOUR(24) {
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
  }, Minute(30) {
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

  Unit(int range) {
    this.range = range;
  }

  public int getRange() {
    return range;
  }

  public abstract LocalDateTime update(Timestamp dateTime, int offset);

  public abstract int getUnit(Timestamp timestamp);

  /**
   * 获取两个Timestamp之间的差值
   * @param before
   * @param after
   * @return
   */
  public abstract int getDifferenceUnit(Timestamp before,Timestamp after);
}
