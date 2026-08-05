package com.crossoverjie.cim.server.handle;

import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.common.kit.HeartBeatHandler;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.common.util.NettyAttrUtil;
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
 * Server 业务 Handler：登录绑定、心跳、断线/超时下线。
 * <p>
 * 【会话绑定】LOGIN_REQUEST → SessionSocketHolder.put(userId, channel)
 * 【心跳】PING → 更新 lastReadTime 并回写心跳包；写失败关 Channel
 * 【心跳超时踢人】READER_IDLE → ServerHeartBeatHandlerImpl：超 cim.heartbeat.time 则清会话并通知 Route
 * 【被动下线】channelInactive → RouteHandler.userOffLine → HTTP /offLine 清 Redis
 * 中间件：SessionSocketHolder（内存）、Route HTTP、Redis（经 Route）
 * 失败：对端 reset 可忽略；其它异常打日志不主动关连（避免误伤）。
 *
 * @author crossoverJie
 *         Date: 17/05/2018 18:52
 * @since JDK 1.8
 */
@ChannelHandler.Sharable
@Slf4j
public class CIMServerHandle extends SimpleChannelInboundHandler<Request> {



    /**
     * 【断线清理链路】Channel 失效时：清本机会话 + 通知 Route 删路由。
     * 失败：userInfo 已为空说明已清理过，跳过（避免重复 offLine）。
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //可能出现业务判断离线后再次触发 channelInactive
        CIMUserInfo userInfo = SessionSocketHolder.getUserId((NioSocketChannel) ctx.channel());
        if (userInfo != null) {
            log.warn("[{}] trigger channelInactive offline!", userInfo.getUserName());

            //Clear route info and offline.
            RouteHandler routeHandler = SpringBeanFactory.getBean(RouteHandler.class);
            routeHandler.userOffLine(userInfo, (NioSocketChannel) ctx.channel());

            ctx.channel().close();
        }
    }

    /**
     * 【心跳检测入口】IdleStateHandler 读空闲事件 → 交给 ServerHeartBeatHandlerImpl 判定是否超时踢人。
     */
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
            // 登录绑定：userId(requestId) ↔ Channel，后续 push 靠此查找
            SessionSocketHolder.put(msg.getRequestId(), (NioSocketChannel) ctx.channel());
            SessionSocketHolder.saveSession(msg.getRequestId(), msg.getReqMsg());
            log.info("client [{}] online success!!", msg.getReqMsg());
        }

        //心跳更新时间
        if (msg.getCmd() == BaseCommand.PING) {
            NettyAttrUtil.updateReaderTime(ctx.channel(), System.currentTimeMillis());
            //向客户端响应 pong 消息
            Request heartBeat = SpringBeanFactory.getBean("heartBeat", Request.class);
            ctx.writeAndFlush(heartBeat).addListeners((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.error("IO error,close Channel");
                    future.channel().close();
                }
            });
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
