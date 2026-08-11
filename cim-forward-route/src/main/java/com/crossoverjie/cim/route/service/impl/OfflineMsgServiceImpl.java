package com.crossoverjie.cim.route.service.impl;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.persistence.api.vo.req.SaveOfflineMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.factory.OfflineMsgFactory;
import com.crossoverjie.cim.route.metric.CimRouteMetrics;
import com.crossoverjie.cim.route.service.DeliveryRetryManager;
import com.crossoverjie.cim.route.service.OfflineMsgService;
import com.crossoverjie.cim.route.service.UserInfoCacheService;
import com.crossoverjie.cim.server.api.ServerApi;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import jakarta.annotation.Resource;
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
 * 【离线拉取】逐条 push（带 messageId）→ SENT + Wheel；禁止推完即 ACKED。
 * 【落库】Snowflake id + PENDING。
 * 【ACK + 指标】markAcked + cancel Wheel → cim.route.delivery.ack +1（CimRouteMetrics）。
 */
@Slf4j
@Service
public class OfflineMsgServiceImpl implements OfflineMsgService {

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

    @Override
    public void fetchOfflineMsgs(CIMServerResVO cimServerResVO, Long receiveUserId) {
        String url = "http://" + cimServerResVO.getIp() + ":" + cimServerResVO.getHttpPort();
        while (true) {
            List<OfflineMsg> offlineMsgs = offlineMsgStore.fetch(receiveUserId);
            if (offlineMsgs == null || offlineMsgs.isEmpty()) {
                break;
            }
            offlineMsgs.sort(Comparator.comparing(OfflineMsg::getMessageId));
            for (OfflineMsg offlineMsg : offlineMsgs) {
                Map<String, String> properties = new HashMap<>();
                if (offlineMsg.getProperties() != null) {
                    properties.putAll(offlineMsg.getProperties());
                }
                properties.put(Constants.MetaKey.MESSAGE_ID, String.valueOf(offlineMsg.getMessageId()));
                properties.put(Constants.MetaKey.RECEIVE_USER_ID, String.valueOf(receiveUserId));
                SendMsgReqVO msgReqVO = SendMsgReqVO.builder()
                        .userId(receiveUserId)
                        .msg(offlineMsg.getContent())
                        .cmd(BaseCommand.OFFLINE)
                        .properties(properties)
                        .build();
                serverApi.sendMsg(msgReqVO, url);
                offlineMsgStore.markSent(receiveUserId, List.of(offlineMsg.getMessageId()));
                deliveryRetryManager.scheduleAfterSent(offlineMsg.getMessageId());
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
}
