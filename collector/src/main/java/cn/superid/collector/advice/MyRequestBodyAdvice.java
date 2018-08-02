package cn.superid.collector.advice;

import cn.superid.collector.annotation.RequestBodyNeedDecrypt;
import cn.superid.collector.util.EncryptionUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdvice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * 对请求body进行解码
 *
 * @author dufeng
 * @create: 2018-07-19 16:37
 */
@RestControllerAdvice
public class MyRequestBodyAdvice implements RequestBodyAdvice {


    private final static Logger logger = LoggerFactory.getLogger(MyRequestBodyAdvice.class);

    /**
     * Invoked first to determine if this interceptor applies.
     *
     * @param methodParameter the method parameter
     * @param type            the target type, not necessarily the same as the method
     *                        parameter type, e.g. for {@code HttpEntity<String>}.
     * @param aClass          the selected converter type
     * @return whether this interceptor should be invoked or not
     */
    @Override
    public boolean supports(MethodParameter methodParameter, Type type, Class<? extends HttpMessageConverter<?>> aClass) {
        Annotation[] methodAnnotations = methodParameter.getExecutable().getDeclaredAnnotations();
        if (methodAnnotations == null) {
            return false;
        }

        for (Annotation annotation : methodAnnotations) {
            //只要controller中的方法上有RequestBodyNeedDecrypt注解，就执行beforeBodyRead方法对其requestbody内容进行解码
            if (annotation.annotationType() == RequestBodyNeedDecrypt.class) {
                return true;
            }
        }

        return false;
    }

    @Override
    public HttpInputMessage beforeBodyRead(HttpInputMessage httpInputMessage, MethodParameter methodParameter, Type type, Class<? extends HttpMessageConverter<?>> aClass) throws IOException {

        try {
            return new MyHttpInputMessage(httpInputMessage);
        } catch (Exception e) {
            e.printStackTrace();
            return httpInputMessage;
        }
    }

    @Override
    public Object afterBodyRead(Object o, HttpInputMessage httpInputMessage, MethodParameter methodParameter, Type type, Class<? extends HttpMessageConverter<?>> aClass) {
        return o;
    }

    @Override
    public Object handleEmptyBody(@Nullable Object o, HttpInputMessage httpInputMessage, MethodParameter methodParameter, Type type, Class<? extends HttpMessageConverter<?>> aClass) {
        return null;
    }


    class MyHttpInputMessage implements HttpInputMessage {
        private HttpHeaders headers;

        private InputStream body;

        public MyHttpInputMessage(HttpInputMessage inputMessage) throws Exception {
            this.headers = inputMessage.getHeaders();

            InputStreamHolder holder = new InputStreamHolder(inputMessage.getBody());

            String bodyStr = IOUtils.toString(holder.getInputStream(), "UTF-8");

            //目前先兼容明文和base64编码的
            //base64编码的
            if (bodyStr.endsWith("=")) {
                byte[] b = EncryptionUtil.base64Decode(bodyStr);
                this.body = IOUtils.toInputStream(IOUtils.toString(b, "UTF-8"), "UTF-8");
            } else {//非base64编码的明文
                this.body = holder.getInputStream();
            }

        }

        @Override
        public InputStream getBody() throws IOException {
            return body;
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }
    }


    public static class InputStreamByteArray {
        private byte[] holder;

        public InputStreamByteArray(InputStream source) throws IOException {
            int length = source.available();
            holder = new byte[length];

            source.read(holder, 0, length);
        }

    }

    /**
     * 缓存InputStream的容器，可以把输入流拿出来重复使用
     */
    public class InputStreamHolder {

        private ByteArrayOutputStream byteArrayOutputStream = null;

        public InputStreamHolder(InputStream source) throws IOException {

            byteArrayOutputStream = new ByteArrayOutputStream();
            //输入流中的字节数组长度
            int length = source.available();
            byte[] holder = new byte[length];
            //把输入流中的字节数组读取到holder中，读取完，输入流source也就不能再用了
            source.read(holder, 0, length);

            //把holder中的字节写到byteArrayOutputStream中
            byteArrayOutputStream.write(holder, 0, length);
            byteArrayOutputStream.flush();

        }

        /**
         * 获取输入流，可以重复使用的方法
         * @return
         */
        public InputStream getInputStream() {
            return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        }
    }
}
