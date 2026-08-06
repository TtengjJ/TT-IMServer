package com.crossoverjie.cim.server.handle;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.common.kit.HeartBeatHandler;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.common.util.NettyAttrUtil;
import com.crossoverjie.cim.route.api.RouteApi;
import com.crossoverjie.cim.route.api.vo.req.AckMsgReqVO;
import com.crossoverjie.cim.server.kit.RouteHandler;
import com.crossoverjie.cim.server.kit.ServerHeartBeatHandlerImpl;
import com.crossoverjie.cim.server.util.SessionSocketHolder;
import com.crossoverjie.cim.server.util.SpringBeanFactory;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * Server 业务 Handler：登录绑定、心跳、ACK、断线/超时下线。
 */
@ChannelHandler.Sharable
@Slf4j
public class CIMServerHandle extends SimpleChannelInboundHandler<Request> {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        CIMUserInfo userInfo = SessionSocketHolder.getUserId((NioSocketChannel) ctx.channel());
        if (userInfo != null) {
            log.warn("[{}] trigger channelInactive offline!", userInfo.getUserName());
            RouteHandler routeHandler = SpringBeanFactory.getBean(RouteHandler.class);
            routeHandler.userOffLine(userInfo, (NioSocketChannel) ctx.channel());
            ctx.channel().close();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
            if (idleStateEvent.state() == IdleState.READER_IDLE) {
                log.info("!!READER_IDLE!!");
                HeartBeatHandler heartBeatHandler = SpringBeanFactory.getBean(ServerHeartBeatHandlerImpl.class);
                heartBeatHandler.process(ctx);
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Request msg) throws Exception {
        log.info("received msg=[{}]", msg.toString());

        if (msg.getCmd() == BaseCommand.LOGIN_REQUEST) {
            SessionSocketHolder.put(msg.getRequestId(), (NioSocketChannel) ctx.channel());
            SessionSocketHolder.saveSession(msg.getRequestId(), msg.getReqMsg());
            log.info("client [{}] online success!!", msg.getReqMsg());
        }

        if (msg.getCmd() == BaseCommand.PING) {
            NettyAttrUtil.updateReaderTime(ctx.channel(), System.currentTimeMillis());
            Request heartBeat = SpringBeanFactory.getBean("heartBeat", Request.class);
            ctx.writeAndFlush(heartBeat).addListeners((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.error("IO error,close Channel");
                    future.channel().close();
                }
            });
        }

        // 【ACK】Client MESSAGE_ACK → HTTP RouteApi.ackMsg → Outbox ACKED + Wheel.cancel
        if (msg.getCmd() == BaseCommand.MESSAGE_ACK) {
            String messageIdStr = msg.getPropertiesMap().get(Constants.MetaKey.MESSAGE_ID);
            if (messageIdStr == null) {
                log.warn("MESSAGE_ACK missing messageId, requestId={}", msg.getRequestId());
                return;
            }
            long receiveUserId = msg.getRequestId();
            String receiveFromProp = msg.getPropertiesMap().get(Constants.MetaKey.RECEIVE_USER_ID);
            if (receiveFromProp != null) {
                receiveUserId = Long.parseLong(receiveFromProp);
            }
            try {
                RouteApi routeApi = SpringBeanFactory.getBean(RouteApi.class);
                routeApi.ackMsg(AckMsgReqVO.builder()
                        .messageId(Long.valueOf(messageIdStr))
                        .receiveUserId(receiveUserId)
                        .build());
                log.info("Forwarded MESSAGE_ACK messageId={}, receiveUserId={}", messageIdStr, receiveUserId);
            } catch (Exception e) {
                log.error("ackMsg failed messageId={}", messageIdStr, e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (CIMException.isResetByPeer(cause.getMessage())) {
            return;
        }
        log.error(cause.getMessage(), cause);
    }
}
