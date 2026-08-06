package com.crossoverjie.cim.persistence.redis;

import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.persistence.redis.kit.OfflineMsgScriptExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.crossoverjie.cim.persistence.redis.constant.Constant.FETCH_OFFLINE_MSG_SIZE;
import static com.crossoverjie.cim.persistence.redis.constant.Constant.OFFLINE_MSG_TTL_DAYS;

/**
 * Redis 离线缓冲。三态/超时重推请用 offline.store.mode=mysql。
 */
@Slf4j
public class OfflineMsgBuffer implements OfflineMsgStore {

    private final int messageTtlDays;
    private final OfflineMsgScriptExecutor scriptExecutor;
    private final ObjectMapper objectMapper;

    public OfflineMsgBuffer(OfflineMsgScriptExecutor scriptExecutor, Integer configuredDays,
                            ObjectMapper objectMapper) {
        this.messageTtlDays = (configuredDays != null && configuredDays > 0) ? configuredDays : OFFLINE_MSG_TTL_DAYS;
        this.scriptExecutor = scriptExecutor;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(OfflineMsg msg) {
        try {
            if (msg.getRetryCount() == null) {
                msg.setRetryCount(0);
            }
            scriptExecutor.saveOfflineMsg(msg, messageTtlDays);
        } catch (SerializationException | RedisException e) {
            log.error("Failed to save offline message", e);
            throw new CIMException(StatusEnum.OFFLINE_MESSAGE_STORAGE_ERROR);
        }
    }

    @Override
    public List<OfflineMsg> fetch(Long userId) {
        try {
            List<String> jsonResult = scriptExecutor.fetchOfflineMsgs(userId, FETCH_OFFLINE_MSG_SIZE);
            return jsonResult.stream().map(json -> {
                try {
                    return objectMapper.readValue(json, OfflineMsg.class);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
        } catch (UncheckedIOException | SerializationException | RedisException e) {
            log.error("Failed to fetch offline messages for userId: {}", userId, e);
            throw new CIMException(StatusEnum.OFFLINE_MESSAGE_FETCH_ERROR);
        }
    }

    @Override
    @Deprecated
    public void markDelivered(Long userId, List<Long> messageIds) {
        delete(userId, messageIds);
    }

    @Override
    public void markSent(Long userId, List<Long> messageIds) {
        log.debug("Redis markSent no-op userId={}", userId);
    }

    @Override
    public void markAcked(Long receiveUserId, Long messageId) {
        if (receiveUserId == null || messageId == null) {
            return;
        }
        delete(receiveUserId, List.of(messageId));
    }

    private void delete(Long userId, List<Long> messageIds) {
        if (CollectionUtils.isEmpty(messageIds)) {
            return;
        }
        try {
            scriptExecutor.deleteOfflineMsg(userId, messageIds);
        } catch (RedisException e) {
            log.error("Failed to delete offline messages for userId: {}", userId, e);
            throw new CIMException(StatusEnum.OFFLINE_MESSAGE_DELETE_ERROR);
        }
    }

    @Override
    public void prepareRetry(Long messageId) {
        log.debug("Redis prepareRetry no-op messageId={}", messageId);
    }

    @Override
    public void markFailed(Long messageId) {
        log.debug("Redis markFailed no-op messageId={}", messageId);
    }

    @Override
    public Optional<OfflineMsg> findByMessageId(Long messageId) {
        return Optional.empty();
    }

    @Override
    public List<OfflineMsg> listNeedRetry(int maxRetry, int limit) {
        return Collections.emptyList();
    }
}
