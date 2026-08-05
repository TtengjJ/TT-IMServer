package com.crossoverjie.cim.server.init;

import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.server.handle.CIMServerHandle;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Server 端 Netty Pipeline 装配。
 * <p>
 * 【管道链路】入口：CIMServer.start → childHandler(本类)
 * 步骤（入站自上而下）：
 *   1. IdleStateHandler(读空闲 11s) → 触发 READER_IDLE，交给 CIMServerHandle 做心跳检测
 *   2. ProtobufVarint32FrameDecoder + ProtobufDecoder(Request) → 拆帧反序列化
 *   3. LengthFieldPrepender + ProtobufEncoder → 出站编码
 *   4. CIMServerHandle → 登录绑定 / PING / 断线清理
 * 中间件：Netty IdleStateHandler、Protobuf
 * 失败：解码异常进 Handler.exceptionCaught；读空闲超时见 ServerHeartBeatHandlerImpl 踢人。
 *
 * @author crossoverJie
 *         Date: 17/05/2018 18:51
 * @since JDK 1.8
 */
public class CIMServerInitializer extends ChannelInitializer<Channel> {

    private final CIMServerHandle cimServerHandle = new CIMServerHandle();

    @Override
    protected void initChannel(Channel ch) throws Exception {

        ch.pipeline()
                // 读空闲 11s：仅触发检测事件；真正踢人阈值见 cim.heartbeat.time
                .addLast(new IdleStateHandler(11, 0, 0))
                // google Protobuf 编解码
                .addLast(new ProtobufVarint32FrameDecoder())
                .addLast(new ProtobufDecoder(Request.getDefaultInstance()))
                .addLast(new ProtobufVarint32LengthFieldPrepender())
                .addLast(new ProtobufEncoder())
                .addLast(cimServerHandle);
    }
}
