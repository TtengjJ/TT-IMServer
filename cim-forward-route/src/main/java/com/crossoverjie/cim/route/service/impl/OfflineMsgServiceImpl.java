package com.crossoverjie.cim.route.service.impl;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.res.BaseResponse;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.persistence.api.vo.req.SaveOfflineMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.config.DeliveryProperties;
import com.crossoverjie.cim.route.factory.OfflineMsgFactory;
import com.crossoverjie.cim.route.metric.CimRouteMetrics;
import com.crossoverjie.cim.route.service.DeliveryRetryManager;
import com.crossoverjie.cim.route.service.OfflineMsgService;
import com.crossoverjie.cim.route.service.UserInfoCacheService;
import com.crossoverjie.cim.server.api.ServerApi;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import com.crossoverjie.cim.server.api.vo.res.SendMsgResVO;
import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * P2P Outbox。
 * <p>
 * 【离线拉取】一批一次 push（正文列表与编号列表等长对齐）→ 按条 SENT + 按条挂 Wheel；禁止推完即 ACKED。
 * 【落库】Snowflake id + PENDING。
 * 【ACK + 指标】markAcked + cancel Wheel → cim.route.delivery.ack +1（CimRouteMetrics）。
 */
@Slf4j
@Service
public class OfflineMsgServiceImpl implements OfflineMsgService {

    private static final int DEFAULT_OFFLINE_BATCH_SIZE = 30;

    @Resource
    private OfflineMsgStore offlineMsgStore;
    @Resource
    private OfflineMsgFactory offlineMsgFactory;
    @Resource
    private UserInfoCacheService userInfoCacheService;
    @Resource
    private ServerApi serverApi;
    @Resource
    private DeliveryRetryManager deliveryRetryManager;
    @Resource
    private CimRouteMetrics cimRouteMetrics;
    @Resource
    private DeliveryProperties deliveryProperties;

    /**
     * 入口：Client 上线 fetchOfflineMsgs。
     * 步骤：按 limit 拉取 → 按 messageId 排序 → 一次 sendMsg(batch) → 成功则按条 markSent + schedule。
     * 失败：Channel 不可达（非 SUCCESS / pushed=false）不 markSent，保持 PENDING 并结束循环，避免空转。
     */
    @Override
    public void fetchOfflineMsgs(CIMServerResVO cimServerResVO, Long receiveUserId) {
        String url = "http://" + cimServerResVO.getIp() + ":" + cimServerResVO.getHttpPort();
        int batchSize = resolveBatchSize();
        while (true) {
            List<OfflineMsg> fetched = offlineMsgStore.fetch(receiveUserId, batchSize);
            if (fetched == null || fetched.isEmpty()) {
                break;
            }
            List<OfflineMsg> offlineMsgs = new ArrayList<>(fetched);
            offlineMsgs.sort(Comparator.comparing(OfflineMsg::getMessageId));
            if (offlineMsgs.size() > batchSize) {
                offlineMsgs = new ArrayList<>(offlineMsgs.subList(0, batchSize));
            }

            List<String> batchMsg = new ArrayList<>(offlineMsgs.size());
            List<Long> batchMessageIds = new ArrayList<>(offlineMsgs.size());
            for (OfflineMsg offlineMsg : offlineMsgs) {
                batchMsg.add(offlineMsg.getContent());
                batchMessageIds.add(offlineMsg.getMessageId());
            }

            Map<String, String> properties = buildSharedProperties(offlineMsgs, receiveUserId);
            SendMsgReqVO msgReqVO = SendMsgReqVO.builder()
                    .userId(receiveUserId)
                    .msg(offlineMsgs.get(0).getContent())
                    .batchMsg(batchMsg)
                    .batchMessageIds(batchMessageIds)
                    .cmd(BaseCommand.OFFLINE)
                    .properties(properties)
                    .build();

            if (!pushBatch(url, receiveUserId, msgReqVO, batchMessageIds)) {
                break;
            }

            offlineMsgStore.markSent(receiveUserId, batchMessageIds);
            for (Long messageId : batchMessageIds) {
                deliveryRetryManager.scheduleAfterSent(messageId);
            }
        }
    }

    @Override
    public OfflineMsg saveOfflineMsg(P2PReqVO p2pRequest) {
        Optional<CIMUserInfo> cimUserInfo = userInfoCacheService.loadUserInfoByUserId(p2pRequest.getUserId());
        if (cimUserInfo.isEmpty()) {
            log.warn("saveOfflineMsg skip, sender not found userId={}", p2pRequest.getUserId());
            return null;
        }
        CIMUserInfo userInfo = cimUserInfo.get();
        SaveOfflineMsgReqVO saveOfflineMsgReqVO = SaveOfflineMsgReqVO.builder()
                .msg(p2pRequest.getMsg())
                .receiveUserId(p2pRequest.getReceiveUserId())
                .properties(Map.of(
                        Constants.MetaKey.SEND_USER_ID, userInfo.getUserId().toString(),
                        Constants.MetaKey.SEND_USER_NAME, userInfo.getUserName(),
                        Constants.MetaKey.RECEIVE_USER_ID, p2pRequest.getReceiveUserId().toString()
                )).build();
        OfflineMsg offlineMsg = offlineMsgFactory.createFromVo(saveOfflineMsgReqVO);
        offlineMsgStore.save(offlineMsg);
        return offlineMsg;
    }

    /**
     * 【ACK 确认链路 + ack 指标】
     * 入口：Client MESSAGE_ACK → CIMServerHandle → RouteApi.ackMsg → 本方法
     * 步骤：markAcked → Wheel.cancel → cim.route.delivery.ack +1
     */
    @Override
    public void ackMsg(Long receiveUserId, Long messageId) {
        offlineMsgStore.markAcked(receiveUserId, messageId);
        deliveryRetryManager.cancel(messageId);
        cimRouteMetrics.deliveryAck();
        log.info("ACKED messageId={}, receiveUserId={}", messageId, receiveUserId);
    }

    @Override
    public void afterPushed(Long receiveUserId, Long messageId) {
        offlineMsgStore.markSent(receiveUserId, List.of(messageId));
        deliveryRetryManager.scheduleAfterSent(messageId);
    }

    private boolean pushBatch(String url, Long receiveUserId, SendMsgReqVO msgReqVO, List<Long> batchMessageIds) {
        try {
            BaseResponse<SendMsgResVO> response = serverApi.sendMsg(msgReqVO, url);
            if (isPushed(response)) {
                return true;
            }
            log.warn("Channel unreachable, keep PENDING receiveUserId={}, messageIds={}",
                    receiveUserId, batchMessageIds);
            return false;
        } catch (Exception e) {
            log.error("offline batch push failed receiveUserId={}, size={}",
                    receiveUserId, batchMessageIds.size(), e);
            return false;
        }
    }

    private static boolean isPushed(BaseResponse<SendMsgResVO> response) {
        if (response == null) {
            return false;
        }
        if (response.getDataBody() != null) {
            return response.getDataBody().isPushed();
        }
        return StatusEnum.SUCCESS.getCode().equals(response.getCode());
    }

    /**
     * 共享 properties 只放公共字段；禁止把第一条 messageId 当成整包主键。
     */
    static Map<String, String> buildSharedProperties(List<OfflineMsg> msgs, Long receiveUserId) {
        Map<String, String> properties = new HashMap<>();
        OfflineMsg first = msgs.get(0);
        if (first.getProperties() != null) {
            properties.putAll(first.getProperties());
        }
        properties.remove(Constants.MetaKey.MESSAGE_ID);
        properties.put(Constants.MetaKey.RECEIVE_USER_ID, String.valueOf(receiveUserId));
        return properties;
    }

    private int resolveBatchSize() {
        if (deliveryProperties == null) {
            return DEFAULT_OFFLINE_BATCH_SIZE;
        }
        int size = deliveryProperties.getOfflineBatchSize();
        return size > 0 ? size : DEFAULT_OFFLINE_BATCH_SIZE;
    }
}
