package com.crossoverjie.cim.server.server;

import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import com.crossoverjie.cim.server.init.CIMServerInitializer;
import com.crossoverjie.cim.server.metric.CimServerMetrics;
import com.crossoverjie.cim.server.util.SessionSocketHolder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CIM Netty Server：长连接接入与消息下发。
 * <p>
 * 【数据面启动】
 * 入口：@PostConstruct start()
 * 步骤：Boss/Worker 绑定 cim.server.port → childHandler=CIMServerInitializer
 * 中间件：Netty NIO、SO_KEEPALIVE
 * 失败：bind 失败抛 InterruptedException；进程退出 @PreDestroy 优雅关线程组。
 * <p>
 * 【消息下发】见 {@link #sendMsg}，由 IndexController HTTP /sendMsg 触发。
 *
 * @author crossoverJie
 *         Date: 21/05/2018 00:30
 * @since JDK 1.8
 */
@Component
@Slf4j
public class CIMServer {


    private EventLoopGroup boss = new NioEventLoopGroup();
    private EventLoopGroup work = new NioEventLoopGroup();


    @Value("${cim.server.port}")
    private int nettyPort;

    @Resource
    private CimServerMetrics cimServerMetrics;


    /**
     * 启动 cim server
     *
     * @return
     * @throws InterruptedException
     */
    @PostConstruct
    public void start() throws InterruptedException {

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, work)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(nettyPort))
                //保持长连接
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new CIMServerInitializer());

        ChannelFuture future = bootstrap.bind().sync();
        if (future.isSuccess()) {
            log.info("Start cim server success!!!");
        }
    }


    /**
     * 销毁
     */
    @PreDestroy
    public void destroy() {
        boss.shutdownGracefully().syncUninterruptibly();
        work.shutdownGracefully().syncUninterruptibly();
        log.info("Close cim server success!!!");
    }


    /**
     * 【消息发送链路·最后一跳 + 推送指标】
     * 入口：Route pushMsg → IndexController.sendMsg → 本方法。
     * 步骤：SessionSocketHolder.get(userId) → 组装 Protobuf Request → writeAndFlush；
     *       Channel==null → cim.server.push{offline}；写出发起 → cim.server.push{success}。
     * 中间件：本机内存会话表、Netty Channel、CimServerMetrics。
     * 失败：Channel 为 null（用户已断）→ 打日志+offline 计数后 return，不抛给 Route。
     */
    public void sendMsg(SendMsgReqVO sendMsgReqVO) {
        NioSocketChannel socketChannel = SessionSocketHolder.get(sendMsgReqVO.getUserId());

        if (null == socketChannel) {
            log.error("client {} offline!", sendMsgReqVO.getUserId());
            cimServerMetrics.pushOffline();
            return;
        }

        Request.Builder requestBuilder = Request.newBuilder()
                .setRequestId(sendMsgReqVO.getUserId())
                .putAllProperties(sendMsgReqVO.getProperties())
                .setCmd(sendMsgReqVO.getCmd());

        boolean isBatch = sendMsgReqVO.getBatchMsg() != null && sendMsgReqVO.getBatchMsg().size() > 0;
        if (isBatch) {
            requestBuilder.addAllBatchReqMsg(sendMsgReqVO.getBatchMsg());
        } else {
            requestBuilder.setReqMsg(sendMsgReqVO.getMsg());
        }

        Request protocol = requestBuilder.build();

        ChannelFuture future = socketChannel.writeAndFlush(protocol);
        cimServerMetrics.pushSuccess();
        future.addListener((ChannelFutureListener) channelFuture ->
                log.info("server push {} msg:[{}], socketChannel:{}", isBatch ? "batch" : "single", sendMsgReqVO, socketChannel));
    }
}
