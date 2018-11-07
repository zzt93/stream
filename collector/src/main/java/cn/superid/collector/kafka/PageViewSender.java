package cn.superid.collector.kafka;

import cn.superid.collector.service.impl.CollectorServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.util.HashMap;

/**
 * @author dufeng
 * @create: 2018-07-24 10:27
 */
public class PageViewSender implements MessageSender {

    private final Logger LOGGER = LoggerFactory.getLogger(PageViewSender.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    private String topic;

    private Object toSendObject;

    public PageViewSender(KafkaTemplate<String, String> kafkaTemplate, String topic, Object toSendObject) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.toSendObject = toSendObject;
    }

    @Override
    public void send() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(KafkaHeaders.TOPIC, topic);
//        LOGGER.info("send msg: " + msg.toString());
//        kafkaTemplate.send(new GenericMessage<>(msg.toString(), map));
    }
}
