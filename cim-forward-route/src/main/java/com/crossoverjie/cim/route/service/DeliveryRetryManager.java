package com.crossoverjie.cim.route.service;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.data.construct.RingBufferWheel;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.config.DeliveryProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * P2P 超时重推：RingBufferWheel。
 * 入口 scheduleAfterSent / cancel / reloadPendingTasks。
 */
@Slf4j
@Component
public class DeliveryRetryManager {

    @Resource
    private OfflineMsgStore offlineMsgStore;
    @Resource
    private AccountService accountService;
    @Resource
    private DeliveryProperties deliveryProperties;

    private RingBufferWheel ringBufferWheel;
    private final ConcurrentHashMap<Long, Integer> messageTaskIds = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        ringBufferWheel = new RingBufferWheel(Executors.newFixedThreadPool(1));
        reloadPendingTasks();
    }

    @PreDestroy
    public void destroy() {
        if (ringBufferWheel != null) {
            ringBufferWheel.stop(true);
        }
    }

    public void scheduleAfterSent(Long messageId) {
        if (messageId == null) {
            return;
        }
        cancel(messageId);
        RetryTask task = new RetryTask(messageId);
        task.setKey(deliveryProperties.getAckTimeoutSeconds());
        int taskId = ringBufferWheel.addTask(task);
        messageTaskIds.put(messageId, taskId);
        log.info("Scheduled delivery retry messageId={}, taskId={}, delay={}s",
                messageId, taskId, deliveryProperties.getAckTimeoutSeconds());
    }

    public void cancel(Long messageId) {
        if (messageId == null) {
            return;
        }
        Integer taskId = messageTaskIds.remove(messageId);
        if (taskId != null) {
            boolean cancelled = ringBufferWheel.cancel(taskId);
            log.info("Cancel delivery retry messageId={}, taskId={}, ok={}", messageId, taskId, cancelled);
        }
    }

    public void reloadPendingTasks() {
        try {
            List<OfflineMsg> list = offlineMsgStore.listNeedRetry(
                    deliveryProperties.getMaxRetry(), deliveryProperties.getReloadLimit());
            for (OfflineMsg msg : list) {
                scheduleAfterSent(msg.getMessageId());
            }
            if (!list.isEmpty()) {
                log.info("Reloaded {} delivery retry tasks from store", list.size());
            }
        } catch (Exception e) {
            log.warn("Reload delivery retry tasks failed: {}", e.getMessage());
        }
    }

    private void doRetry(Long messageId) {
        messageTaskIds.remove(messageId);
        Optional<OfflineMsg> optional = offlineMsgStore.findByMessageId(messageId);
        if (optional.isEmpty()) {
            return;
        }
        OfflineMsg msg = optional.get();
        if (msg.getStatus() != null && (msg.getStatus() == Constants.DeliveryStatus.ACKED
                || msg.getStatus() == Constants.DeliveryStatus.FAILED)) {
            return;
        }
        int retryCount = msg.getRetryCount() == null ? 0 : msg.getRetryCount();
        if (retryCount >= deliveryProperties.getMaxRetry()) {
            offlineMsgStore.markFailed(messageId);
            return;
        }
        offlineMsgStore.prepareRetry(messageId);
        Optional<CIMServerResVO> route = accountService.loadRouteRelatedByUserId(msg.getReceiveUserId());
        if (route.isEmpty()) {
            return;
        }
        String sendUserId = msg.getProperties() == null ? null
                : msg.getProperties().get(Constants.MetaKey.SEND_USER_ID);
        if (sendUserId == null) {
            return;
        }
        ChatReqVO chatVO = new ChatReqVO(msg.getReceiveUserId(), msg.getContent(), null);
        chatVO.setMessageId(messageId);
        accountService.pushMsg(route.get(), Long.parseLong(sendUserId), chatVO);
        offlineMsgStore.markSent(msg.getReceiveUserId(), List.of(messageId));
        scheduleAfterSent(messageId);
    }

    private final class RetryTask extends RingBufferWheel.Task {
        private final Long messageId;

        private RetryTask(Long messageId) {
            this.messageId = messageId;
        }

        @Override
        public void run() {
            try {
                doRetry(messageId);
            } catch (Exception e) {
                log.error("Delivery retry error messageId={}", messageId, e);
            }
        }
    }
}
