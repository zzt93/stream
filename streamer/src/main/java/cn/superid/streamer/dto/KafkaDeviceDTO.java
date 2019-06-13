package cn.superid.streamer.dto;

import lombok.Data;

@Data
public class KafkaDeviceDTO {
    private Long userId;

    private String deviceId;

    private String agent;
}
