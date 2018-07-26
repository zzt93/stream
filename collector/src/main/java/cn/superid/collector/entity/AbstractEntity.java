package cn.superid.collector.entity;

import com.google.gson.annotations.Expose;
import org.springframework.data.annotation.Transient;

import java.util.regex.Pattern;

/**
 * @author dufeng
 * @create: 2018-07-19 09:39
 */
public abstract class AbstractEntity {

    /**
     * 为了不让MongoTemplate插入到mongo的时候把该字段插入，加上Spring提供的@Transient注解
     * 为了不让发送kafka的时候调用实体类的toString方法的时候序列化该字段，需要加上transient修饰
     */
    @Transient
    private transient Pattern ipPattern = Pattern.compile("^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."

            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."

            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."

            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$");


    @Transient
    private transient Pattern timeStampPattern = Pattern.compile("[0-9]{10,13}");


    public boolean validIp(String ip) {
        if (ip == null || !ipPattern.matcher(ip).matches()) {
            return false;
        }
        return true;
    }

    public boolean validTimestamp(String timeStr) {
        if (timeStr == null || !timeStampPattern.matcher(timeStr).matches()) {
            return false;
        }
        return true;
    }


    /**
     * 对上报数据进行合法性校验
     * @return
     */
    abstract boolean validate();
}
