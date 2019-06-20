package cn.superid.streamer.listener;

import cn.superid.streamer.constant.OperationType;
import cn.superid.streamer.dto.KafkaDeviceDTO;
import cn.superid.streamer.service.StreamerService;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaDeviceEventListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDeviceEventListener.class);

    private static final String TOPIC_ONLINE = "device_online";
    private static final String TOPIC_OFFLINE = "device_offline";

    @Autowired
    private StreamerService streamerService;

    @KafkaListener(topics = "${spring.kafka.topic-prefix}" + TOPIC_OFFLINE)
    public void DeviceOfflineListener(ConsumerRecord<Long, String> record) {
        logger.info("deviceOfflineListener, record={}", record.toString());
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);
        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.offline, record.timestamp());
        }
    }

    @KafkaListener(topics = "${spring.kafka.topic-prefix}" + TOPIC_ONLINE)
    public void DeviceOnlineListener(ConsumerRecord<Long, String> record) {
        logger.info("deviceOnlineListener, record={}", record.toString());
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);

        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.online, record.timestamp());
        }
    }

}
