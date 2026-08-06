package com.crossoverjie.cim.route.service;

import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;

public interface OfflineMsgService {

    void fetchOfflineMsgs(CIMServerResVO cimServerResVO, Long receiveUserId);

    OfflineMsg saveOfflineMsg(P2PReqVO p2pRequest);

    void ackMsg(Long receiveUserId, Long messageId);

    void afterPushed(Long receiveUserId, Long messageId);
}
