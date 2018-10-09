package cn.superid.collector.util;

import org.springframework.util.StringUtils;

/**
 * @author dufeng
 * @create: 2018-10-09 11:30
 */
public class IpUtil {

    public static String getIpFrom(String str){
        if(StringUtils.isEmpty(str)){
            return "empty";
        }
        //多层转发的时候，第一个才是客户端的ip地址
        return str.split(",")[0];
    }

}
