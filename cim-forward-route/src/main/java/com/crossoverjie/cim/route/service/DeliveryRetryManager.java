package com.crossoverjie.cim.route.service;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.data.construct.RingBufferWheel;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.config.DeliveryProperties;
import com.crossoverjie.cim.route.metric.CimRouteMetrics;
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
 * P2P 超时重推：RingBufferWheel + 重推指标。
 * <p>
 * 【调度 cim.route.delivery.retry{scheduled}】
 * 入口：afterPushed / fetchOfflineMsgs / doRetry 再次登记 → scheduleAfterSent
 * 步骤：cancel 旧任务 → Wheel.addTask(ack-timeout) → retry{scheduled} +1
 * <p>
     * 【执行 cim.route.delivery.retry{executed}】
     * 入口：时间轮到期触发 RetryTask.run
     * 步骤：retry{executed} +1 → doRetry（按单个 messageId 查库/重推/再 schedule，禁止整包重推）
 * <p>
 * 【取消】ACK 成功 → cancel，不再 executed。
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
    @Resource
    private CimRouteMetrics cimRouteMetrics;

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

    /**
     * 登记 ACK 超时重推任务，并计 scheduled。
     */
    public void scheduleAfterSent(Long messageId) {
        if (messageId == null) {
            return;
        }
        cancel(messageId);
        RetryTask task = new RetryTask(messageId);
        task.setKey(deliveryProperties.getAckTimeoutSeconds());
        int taskId = ringBufferWheel.addTask(task);
        messageTaskIds.put(messageId, taskId);
        cimRouteMetrics.retryScheduled();
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
        // 超时只重推未确认的单条：按 messageId 查库再推，不为整批建一个时间轮任务。
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
            // 任务真正跑起来即计 executed（无论后续是否因已 ACKED 提前 return）
            cimRouteMetrics.retryExecuted();
            try {
                doRetry(messageId);
            } catch (Exception e) {
                log.error("Delivery retry error messageId={}", messageId, e);
            }
        }
    }
}
