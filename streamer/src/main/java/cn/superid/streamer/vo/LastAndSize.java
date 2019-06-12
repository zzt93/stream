package cn.superid.streamer.vo;

import java.sql.Timestamp;

public class LastAndSize {
    private Timestamp last;
    private int size;

    public LastAndSize() {
    }

    public LastAndSize(Timestamp last, int size) {
        this.last = last;
        this.size = size;
    }

    public Timestamp getLast() {
        return last;
    }

    public void setLast(Timestamp last) {
        this.last = last;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
