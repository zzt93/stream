package cn.superid.collector.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标识请求体是否需要解密的注解
 * @author dufeng
 * @create: 2018-07-19 17:37
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequestBodyNeedDecrypt {

}
