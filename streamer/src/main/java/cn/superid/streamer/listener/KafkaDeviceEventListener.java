package cn.superid.streamer.listener;

import cn.superid.streamer.constant.OperationType;
import cn.superid.streamer.dto.KafkaDeviceDTO;
import cn.superid.streamer.service.StreamerService;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 *
 * @author yinywf
 * @version $Id: KafkaDeviceEventListener.java, v 0.1 2019年04月12日
 */

@Component
public class KafkaDeviceEventListener {

    private static final String TOPIC_ONLINE = "device_online";
    private static final String TOPIC_OFFLINE = "device_offline";

    @Autowired
    private StreamerService streamerService;

    @KafkaListener(topics = "${spring.kafka.topic-prefix}" + TOPIC_OFFLINE,
            containerFactory = "longKeyFactory")
    public void deviceOfflineListener(ConsumerRecord<Long, String> record,
                                      Acknowledgment ack) throws Exception {
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);
        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.offline);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = "${spring.kafka.topic-prefix}" + TOPIC_ONLINE,
            containerFactory = "longKeyFactory")
    public void deviceOnlineListener(ConsumerRecord<Long, String> record,
                                     Acknowledgment ack) throws Exception {
        KafkaDeviceDTO deviceDTO = JSON.parseObject(record.value(), KafkaDeviceDTO.class);

        if (deviceDTO != null) {
            streamerService.userOperation(deviceDTO, OperationType.online);
        }
        ack.acknowledge();
    }

}
