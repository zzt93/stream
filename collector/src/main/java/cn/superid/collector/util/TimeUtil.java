package cn.superid.collector.util;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author dufeng
 * @create: 2018-07-19 09:50
 */
public class TimeUtil {

    private static final DateTimeFormatter dateTimeFormatter= DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");

    public static String getDateTimeStr(long timestampSecond){
        String result = dateTimeFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochSecond(timestampSecond), ZoneId.of("Asia/Shanghai")));
        return result;
    }

    public static String getDateTimeStr(LocalDateTime localDateTime){
        String result = dateTimeFormatter.format(localDateTime);
        return result;
    }

//    public static void main(String[] args) {
//        LocalDateTime now = LocalDateTime.now();
//
//        System.out.println(now);
//        System.out.println(Timestamp.valueOf(now));
//        System.out.println(TimeUtil.getDateTimeStr(now));
//    }
}
