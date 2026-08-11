package com.crossoverjie.cim.server.kit;

import com.crossoverjie.cim.common.kit.HeartBeatHandler;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.util.NettyAttrUtil;
import com.crossoverjie.cim.server.config.AppConfiguration;
import com.crossoverjie.cim.server.metric.CimServerMetrics;
import com.crossoverjie.cim.server.util.SessionSocketHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 【心跳超时踢人 + 踢人指标】
 * 入口：CIMServerHandle.userEventTriggered(READER_IDLE)
 * 步骤：比较 now - lastReadTime 与 cim.heartbeat.time；
 *       超时则 cim.server.heartbeat.kick +1 → userOffLine + close
 * 中间件：Channel 属性 lastReadTime、RouteHandler → Route/Redis、CimServerMetrics
 * 失败：未超时则忽略（仅读空闲探测，不计数）；超时后 Client channelInactive 可触发重连。
 *
 * @author crossoverJie
 * Date: 2019-01-20 17:16
 * @since JDK 1.8
 */
@Service
@Slf4j
public class ServerHeartBeatHandlerImpl implements HeartBeatHandler {


    @Autowired
    private RouteHandler routeHandler;

    @Autowired
    private AppConfiguration appConfiguration;

    @Autowired
    private CimServerMetrics cimServerMetrics;

    @Override
    public void process(ChannelHandlerContext ctx) throws Exception {

        long heartBeatTime = appConfiguration.getHeartBeatTime() * 1000;

        Long lastReadTime = NettyAttrUtil.getReaderTime(ctx.channel());
        long now = System.currentTimeMillis();
        if (lastReadTime != null && now - lastReadTime > heartBeatTime) {
            CIMUserInfo userInfo = SessionSocketHolder.getUserId((NioSocketChannel) ctx.channel());
            if (userInfo != null) {
                log.warn("客户端[{}]心跳超时[{}]ms，需要关闭连接!", userInfo.getUserName(), now - lastReadTime);
            }
            cimServerMetrics.heartbeatKick();
            routeHandler.userOffLine(userInfo, (NioSocketChannel) ctx.channel());
            ctx.channel().close();
        }
    }
}
