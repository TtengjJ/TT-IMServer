package com.crossoverjie.cim.client.sdk.io;

import com.crossoverjie.cim.client.sdk.ClientState;
import com.crossoverjie.cim.client.sdk.impl.ClientImpl;
import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.common.protocol.Response;
import com.crossoverjie.cim.common.util.NettyAttrUtil;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Client Netty Handler。
 * <p>
 * 【可靠投递接收】非 PING：Dispatcher 规划 → 合法批量按下标去重并按条 ACK；非法批量整包丢弃。
 */
@ChannelHandler.Sharable
@Slf4j
public class CIMClientHandle extends SimpleChannelInboundHandler<Response> {

    private final Map<Long, MessageIdDeduper> deduperByUser = new ConcurrentHashMap<>();

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleStateEvent) {
            if (idleStateEvent.state() == IdleState.WRITER_IDLE) {
                ctx.writeAndFlush(ClientImpl.getClient().getHeartBeatPacket())
                        .addListeners((ChannelFutureListener) future -> {
                            if (!future.isSuccess()) {
                                log.error("heart beat error,close Channel");
                                ClientImpl.getClient().getConf().getEvent().warn("heart beat error,close Channel");
                                future.channel().close();
                            }
                        });
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ClientImpl.getClient().getConf().getEvent().debug("ChannelActive");
        ClientImpl.getClient().setState(ClientState.State.Ready);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (!ClientImpl.getClient().getConf().getReconnectCheck().isNeedReconnect(ClientImpl.getClient())) {
            return;
        }
        ClientImpl.getClient().setState(ClientState.State.Closed);
        ClientImpl.getClient().getConf().getEvent().warn("Client inactive, let's reconnect");
        ClientImpl.getClient().getReConnectManager().reConnect(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Response msg) {
        if (msg.getCmd() == BaseCommand.PING) {
            ClientImpl.getClient().getConf().getEvent().debug("received ping from server");
            NettyAttrUtil.updateReaderTime(ctx.channel(), System.currentTimeMillis());
            return;
        }

        String receiveUserIdStr = msg.getPropertiesMap().get(Constants.MetaKey.RECEIVE_USER_ID);
        if (receiveUserIdStr == null) {
            log.error("receiveUserId missing in properties");
            return;
        }
        ClientImpl client = ClientImpl.getClientMap().get(Long.valueOf(receiveUserIdStr));
        if (client == null) {
            log.error("client not found for userId: {}", receiveUserIdStr);
            return;
        }

        client.getConf().getCallbackThreadPool().execute(() -> {
            MessageListener messageListener = client.getConf().getMessageListener();
            MessageIdDeduper deduper = deduperByUser.computeIfAbsent(
                    client.getAuth().getUserId(), id -> new MessageIdDeduper());

            ReliableDeliveryDispatcher.DispatchPlan plan = ReliableDeliveryDispatcher.plan(msg, deduper);
            if (plan.isInvalid()) {
                log.error("Invalid batch packet: bodyCount={}, idCount={}, skip callback and ack",
                        msg.getBatchResMsgCount(), msg.getBatchMessageIdCount());
                return;
            }

            for (ReliableDeliveryDispatcher.DispatchItem item : plan.getItems()) {
                if (item.shouldCallback()) {
                    Map<String, String> props = new HashMap<>(msg.getPropertiesMap());
                    if (item.getMessageId() != null) {
                        props.put(Constants.MetaKey.MESSAGE_ID, String.valueOf(item.getMessageId()));
                    }
                    messageListener.received(client, props, item.getBody());
                } else if (item.isDuplicate()) {
                    log.info("Duplicate messageId={}, skip business callback", item.getMessageId());
                }
                // 按条确认；一次确认多个编号本任务不做
                if (item.shouldAck() && item.getAckMessageId() != null) {
                    sendAck(ctx, client.getAuth().getUserId(), item.getAckMessageId());
                }
            }
        });
    }

    private void sendAck(ChannelHandlerContext ctx, Long userId, String messageIdStr) {
        Request ack = Request.newBuilder()
                .setRequestId(userId)
                .setCmd(BaseCommand.MESSAGE_ACK)
                .putProperties(Constants.MetaKey.MESSAGE_ID, messageIdStr)
                .putProperties(Constants.MetaKey.RECEIVE_USER_ID, String.valueOf(userId))
                .build();
        ctx.writeAndFlush(ack).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.warn("MESSAGE_ACK write failed messageId={}", messageIdStr);
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ClientImpl.getClient().getConf().getEvent().error(cause.getCause().toString());
        ctx.close();
    }
}
