package com.crossoverjie.cim.route.api;

import com.crossoverjie.cim.common.core.proxy.Request;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.res.BaseResponse;
import com.crossoverjie.cim.common.res.NULLBody;
import com.crossoverjie.cim.route.api.vo.req.AckMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.req.LoginReqVO;
import com.crossoverjie.cim.route.api.vo.req.OfflineMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.req.RegisterInfoReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.api.vo.res.RegisterInfoResVO;
import com.crossoverjie.cim.route.api.vo.res.SendMsgResVO;

import java.util.Set;

public interface RouteApi {

    BaseResponse<NULLBody> groupRoute(ChatReqVO groupReqVO);

    /**
     * 【P2P】先落 Outbox PENDING；在线再 push→SENT+Wheel；返回 messageId。
     */
    BaseResponse<SendMsgResVO> p2pRoute(P2PReqVO p2pRequest);

    BaseResponse<NULLBody> offLine(ChatReqVO groupReqVO);

    /**
     * 【ACK】Client MESSAGE_ACK → Server → /ackMsg → ACKED + Wheel.cancel
     */
    BaseResponse<NULLBody> ackMsg(AckMsgReqVO ackMsgReqVO);

    BaseResponse<CIMServerResVO> login(LoginReqVO loginReqVO) throws Exception;

    BaseResponse<RegisterInfoResVO> registerAccount(RegisterInfoReqVO registerInfoReqVO) throws Exception;

    @Request(method = Request.GET)
    BaseResponse<Set<CIMUserInfo>> onlineUser() throws Exception;

    BaseResponse<NULLBody> fetchOfflineMsgs(OfflineMsgReqVO offlineMsgReqVO);
}
