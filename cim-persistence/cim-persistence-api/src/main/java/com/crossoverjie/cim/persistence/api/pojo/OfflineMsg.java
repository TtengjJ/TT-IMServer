package com.crossoverjie.cim.persistence.api.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * P2P 投递 Outbox 记录（表 offline_msg；在线/离线共用）。
 * status：0 PENDING / 1 SENT / 2 ACKED / 3 FAILED
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OfflineMsg {
    private Long messageId;
    private Long receiveUserId;
    private String content;
    private Integer messageType;
    private Integer status;
    private Integer retryCount;
    private LocalDateTime createdAt;
    private Map<String, String> properties;
}
