package com.crossoverjie.cim.server.controller;

import com.crossoverjie.cim.common.core.proxy.DynamicUrl;
import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.res.BaseResponse;
import com.crossoverjie.cim.server.api.ServerApi;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import com.crossoverjie.cim.server.api.vo.res.SendMsgResVO;
import com.crossoverjie.cim.server.server.CIMServer;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * cim-server HTTP 入口（实现 ServerApi），供 Route 推送消息。
 * <p>
 * 【消息发送链路】
 * 入口：POST /sendMsg（Route AccountServiceRedisImpl.pushMsg 经 RpcProxy 调用）
 * 步骤：本方法 → CIMServer.sendMsg → SessionSocketHolder 取 Channel → Netty 写出 MESSAGE
 *       （推送成功/离线计数在 CIMServer.sendMsg → CimServerMetrics，本层不再打点）
 * 中间件：Spring MVC、Netty；@DynamicUrl 指定目标 Server 的 http 地址
 * 失败：本机无 Channel 时返回 OFF_LINE（pushed=false），Route 离线补发据此不 markSent。
 *
 * @author crossoverJie
 *         Date: 22/05/2018 14:46
 * @since JDK 1.8
 */
@Controller
@RequestMapping("/")
public class IndexController implements ServerApi {

    @Autowired
    private CIMServer cimServer;


    /**
     *
     * @param sendMsgReqVO
     * @return
     */
    @Override
    @Operation(summary = "Push msg to client")
    @RequestMapping(value = "sendMsg", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponse<SendMsgResVO> sendMsg(@RequestBody SendMsgReqVO sendMsgReqVO, @DynamicUrl String url) {
        BaseResponse<SendMsgResVO> res = new BaseResponse<>();
        boolean pushed = cimServer.sendMsg(sendMsgReqVO);

        SendMsgResVO sendMsgResVO = new SendMsgResVO();
        sendMsgResVO.setMsg("OK");
        sendMsgResVO.setPushed(pushed);
        if (pushed) {
            res.setCode(StatusEnum.SUCCESS.getCode());
            res.setMessage(StatusEnum.SUCCESS.getMessage());
        } else {
            res.setCode(StatusEnum.OFF_LINE.getCode());
            res.setMessage(StatusEnum.OFF_LINE.getMessage());
        }
        res.setDataBody(sendMsgResVO);
        return res;
    }

}
