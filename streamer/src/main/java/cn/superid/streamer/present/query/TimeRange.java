package cn.superid.streamer.present.query;

import java.sql.Timestamp;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

/**
 * @author zzt
 */
public class TimeRange {

  private static final int LIMIT = 1000;
  private Timestamp from;
  private Timestamp to;
  private int page;
  private int size;

  public Timestamp getFrom() {
    return from;
  }

  public void setFrom(Timestamp from) {
    this.from = from;
  }

  public Timestamp getTo() {
    return to;
  }

  public void setTo(Timestamp to) {
    this.to = to;
  }

  public int getPage() {
    return page;
  }

  public void setPage(int page) {
    this.page = page;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public Pageable pageRequest() {
    if (size != 0) {
      PageRequest request = PageRequest.of(page, size);
      if (request.getOffset() > LIMIT) {
        return null;
      }
      return request;
    }
    return Pageable.unpaged();
  }
}
