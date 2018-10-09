package cn.superid.collector.util;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.util.regex.Pattern;

/**
 * 用于判断客户端的设备类型
 *
 * @author dufeng
 * @create: 2018-10-09 16:25
 */
public class DevUtil {
    /**
     * windows设备
     */
    private static final Pattern WINDOWS = Pattern.compile("Windows", Pattern.CASE_INSENSITIVE);
    /**
     * iPhone设备
     */
    private static final Pattern IPHONE = Pattern.compile("iPhone", Pattern.CASE_INSENSITIVE);
    /**
     * Mac设备
     */
    private static final Pattern MAC = Pattern.compile("Mac", Pattern.CASE_INSENSITIVE);
    /**
     * Android设备
     */
    private static final Pattern ANDROID = Pattern.compile("Android", Pattern.CASE_INSENSITIVE);
    /**
     * IOS设备（iPad？）
     */
    private static final Pattern IOS = Pattern.compile("IOS", Pattern.CASE_INSENSITIVE);


    public static String getDeviceType(String userAgent) {
        if (WINDOWS.matcher(userAgent).find()) {
            return "Windows";
        } else if (IPHONE.matcher(userAgent).find()) {
            return "iPhone";
        } else if (MAC.matcher(userAgent).find()) {
            return "Mac";
        } else if (ANDROID.matcher(userAgent).find()) {
            return "Android";
        } else if (IOS.matcher(userAgent).find()) {
            return "IOS";
        } else {
            return "Unknown";
        }

    }
}
