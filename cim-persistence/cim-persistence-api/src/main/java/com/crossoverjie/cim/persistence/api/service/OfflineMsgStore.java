package com.crossoverjie.cim.persistence.api.service;

import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;

import java.util.List;
import java.util.Optional;

/**
 * P2P 投递 Outbox。可靠投递三态以 MySQL 为准；Redis 仅基本存取/ACK 删除。
 */
public interface OfflineMsgStore {

    void save(OfflineMsg offlineMsg);

    List<OfflineMsg> fetch(Long userId);

    @Deprecated
    void markDelivered(Long userId, List<Long> messageIds);

    void markSent(Long userId, List<Long> messageIds);

    void markAcked(Long receiveUserId, Long messageId);

    void prepareRetry(Long messageId);

    void markFailed(Long messageId);

    Optional<OfflineMsg> findByMessageId(Long messageId);

    List<OfflineMsg> listNeedRetry(int maxRetry, int limit);
}
