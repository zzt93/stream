package cn.superid.streamer.listener;

import cn.superid.streamer.constant.OperationType;
import cn.superid.streamer.dto.KafkaDeviceDTO;
import cn.superid.streamer.service.StreamerService;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author yinywf
 * @version $Id: KafkaDeviceEventListener.java, v 0.1 2019年04月12日
 */

@Component
public class KafkaDeviceEventListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDeviceEventListener.class);

    private static final String TOPIC_ONLINE = "device_online";
    private static final String TOPIC_OFFLINE = "device_offline";

    @Autowired
    private StreamerService streamerService;

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    @Bean("ackContainerFactory")
    public ConcurrentKafkaListenerContainerFactory ackContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @KafkaListener(id = "streamer", topics = "release_" + TOPIC_OFFLINE,
            containerFactory = "ackContainerFactory")
    public void deviceOfflineListener(ConsumerRecord<Long, String> record,
                                      Acknowledgment ack) throws Exception {
        logger.info("deviceOfflineListener, record={}", record.toString());
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);
        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.offline);
        }
        ack.acknowledge();
    }

    @KafkaListener(id = "streamer", topics = "release_" + TOPIC_ONLINE,
            containerFactory = "ackContainerFactory")
    public void deviceOnlineListener(ConsumerRecord<Long, String> record,
                                     Acknowledgment ack) throws Exception {
        logger.info("deviceOnlineListener, record={}", record.toString());
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);

        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.online);
        }
        ack.acknowledge();
    }

}
