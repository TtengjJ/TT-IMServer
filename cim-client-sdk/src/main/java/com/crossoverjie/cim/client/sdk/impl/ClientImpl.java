package com.crossoverjie.cim.client.sdk.impl;

import static com.crossoverjie.cim.common.enums.StatusEnum.RECONNECT_FAIL;

import com.crossoverjie.cim.client.sdk.Client;
import com.crossoverjie.cim.client.sdk.ClientState;
import com.crossoverjie.cim.client.sdk.FetchOfflineMsgJob;
import com.crossoverjie.cim.client.sdk.ReConnectManager;
import com.crossoverjie.cim.client.sdk.RouteManager;
import com.crossoverjie.cim.client.sdk.io.CIMClientHandleInitializer;
import com.crossoverjie.cim.common.data.construct.RingBufferWheel;
import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.common.kit.HeartBeatHandler;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.req.LoginReqVO;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Client SDK 核心实现：登录选服 → Netty 建连 → 发 LOGIN_REQUEST 绑定会话。
 * <p>
 * 【客户端登录选服链路】
 * 入口：构造函数 → connectServer → doConnectServer
 * 步骤：
 *   1. RouteManager.getServer → HTTP /login（Redis 鉴权 + ZK 选服）
 *   2. Bootstrap.connect(ip, cimPort) 建立长连接
 *   3. loginServer 发送 Protobuf LOGIN_REQUEST，Server 写入 SessionSocketHolder
 * 中间件：HTTP(Route)、Netty、Protobuf
 * 失败：拿不到 Server → fatal；连接/登录异常累计超 loginRetryCount → fatal；
 *       断线后可由 ReConnectManager 触发 reconnect（先 offLine 再重新选服）。
 * <p>
 * 发消息不走 Netty 上行，而走 RouteManager → Route HTTP（见 sendP2P / sendGroup）。
 */
@Slf4j
public class ClientImpl extends ClientState implements Client {

    @Getter
    private final ClientConfigurationData conf;
    private static final int CALLBACK_QUEUE_SIZE = 1024;
    private static final int CALLBACK_POOL_SIZE = 10;

    // ======= private ========
    private int errorCount;
    private SocketChannel channel;

    private final RouteManager routeManager;

    @Getter
    private final HeartBeatHandler heartBeatHandler = ReConnectManager.createHeartBeatHandler();
    @Getter
    private final ReConnectManager reConnectManager = ReConnectManager.createReConnectManager();

    @Getter
    private static ClientImpl client;
    @Getter
    private static Map<Long, ClientImpl> clientMap = new ConcurrentHashMap<>();
    @Getter
    private final Request heartBeatPacket;

    private RingBufferWheel ringBufferWheel;

    // Client connected server info
    private CIMServerResVO serverInfo;

    public ClientImpl(ClientConfigurationData conf) {
        this.conf = conf;

        if (this.conf.getCallbackThreadPool() == null) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(CALLBACK_QUEUE_SIZE);
            ThreadFactory factory = new ThreadFactoryBuilder()
                    .setNameFormat("msg-callback-%d")
                    .setDaemon(true)
                    .build();
            this.conf.setCallbackThreadPool(
                    new ThreadPoolExecutor(CALLBACK_POOL_SIZE, CALLBACK_POOL_SIZE, 1, TimeUnit.SECONDS, queue,
                            factory));
        }

        routeManager = new RouteManager(conf.getRouteUrl(), conf.getOkHttpClient(), conf.getEvent());

        heartBeatPacket = Request.newBuilder()
                .setRequestId(this.conf.getAuth().getUserId())
                .setReqMsg("ping")
                .setCmd(BaseCommand.PING)
                .build();
        client = this;
        clientMap.put(conf.getAuth().getUserId(), this);

        connectServer(v -> this.conf.getEvent().info("Login success!"));

        postConnectionSetup();
    }

    /**
     * 1. Pull offline messages from the server
     */
    private void postConnectionSetup() {
        ringBufferWheel = new RingBufferWheel(Executors.newFixedThreadPool(1));
        ringBufferWheel.addTask(new FetchOfflineMsgJob(routeManager, conf));
    }

    private void connectServer(Consumer<Void> success) {
        this.doConnectServer().whenComplete((r, e) -> {
            if (r) {
                success.accept(null);
            }
            if (e != null) {
                if (e instanceof CIMException cimException && cimException.getErrorCode()
                        .equals(RECONNECT_FAIL.getCode())) {
                    this.conf.getEvent().fatal(this);
                } else {
                    if (errorCount++ >= this.conf.getLoginRetryCount()) {
                        this.conf.getEvent()
                                .error("The maximum number of reconnections has been reached[{}]times, exit cim client!",
                                        errorCount);
                        this.conf.getEvent().fatal(this);
                    }
                }
            }

        });
    }

    /**
     * 登录选服三步：HTTP 选服 → TCP 连 Server → 发 LOGIN_REQUEST。
     * 任一步失败：无 Server 则 fatal；异常由 whenComplete / retry 处理。
     */
    private CompletableFuture<Boolean> doConnectServer() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        this.userLogin(future).ifPresentOrElse((cimServer) -> {
            this.doConnectServer(cimServer, future);
            this.loginServer();
            this.serverInfo = cimServer;
            future.complete(true);
        }, () -> {
            this.conf.getEvent().error("Login fail!, cannot get server info!");
            this.conf.getEvent().fatal(this);
            future.complete(false);
        });
        return future;
    }

    /**
     * 步骤1：向 Route 登录并获取目标 CIMServerResVO。
     */
    private Optional<CIMServerResVO> userLogin(CompletableFuture<Boolean> future) {
        LoginReqVO loginReqVO = new LoginReqVO(conf.getAuth().getUserId(),
                conf.getAuth().getUserName());

        CIMServerResVO cimServer = null;
        try {
            cimServer = routeManager.getServer(loginReqVO);
            log.info("cimServer=[{}]", cimServer);
        } catch (Exception e) {
            log.error("login fail", e);
            future.completeExceptionally(e);
        }
        return Optional.ofNullable(cimServer);
    }

    private final EventLoopGroup group = new NioEventLoopGroup(0, new DefaultThreadFactory("cim-work"));

    /**
     * 步骤2：Netty 连接 Route 返回的 cimServerPort（数据面，非 HTTP 端口）。
     */
    private void doConnectServer(CIMServerResVO cimServer, CompletableFuture<Boolean> future) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new CIMClientHandleInitializer());
        ChannelFuture sync;
        try {
            sync = bootstrap.connect(cimServer.getIp(), cimServer.getCimServerPort()).sync();
            if (sync.isSuccess()) {
                this.conf.getEvent().info("Start cim client success!");
                channel = (SocketChannel) sync.channel();
            }
        } catch (InterruptedException e) {
            future.completeExceptionally(e);
        }
    }

    /**
     * 步骤3：发送 LOGIN_REQUEST，Server 侧绑定 userId↔Channel。
     */
    private void loginServer() {
        Request login = Request.newBuilder()
                .setRequestId(this.conf.getAuth().getUserId())
                .setReqMsg(this.conf.getAuth().getUserName())
                .setCmd(BaseCommand.LOGIN_REQUEST)
                .build();
        channel.writeAndFlush(login)
                .addListener((ChannelFutureListener) channelFuture ->
                        this.conf.getEvent().info("Registry cim server success!")
                );
    }

    /**
     * 【重连链路】清 Route 路由 → backoff → 重新 doConnectServer（再次 /login 选服）。
     * 失败：达到重试上限或 RECONNECT_FAIL → Event.fatal。
     */
    public void reconnect() throws Exception {
        if (channel != null && channel.isActive()) {
            return;
        }
        this.serverInfo = null;
        // clear route information.
        this.routeManager.offLine(this.getConf().getAuth().getUserId());

        this.conf.getEvent().info("cim trigger reconnecting....");

        this.conf.getBackoffStrategy().runBackoff();

        // don't set State ready, because when connect success, the State will be set to ready automate.
        connectServer(v -> {
            this.reConnectManager.reConnectSuccess();
            this.conf.getEvent().info("Great! reConnect success!!!");
        });
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
        super.setState(ClientState.State.Closed);
        this.routeManager.offLine(this.getAuth().getUserId());
        this.clientMap.remove(this.getAuth().getUserId());
        ringBufferWheel.stop(true);
    }

    @Override
    public void sendP2P(P2PReqVO p2PReqVO) throws Exception {
        recordSendLog(sendP2PAsync(p2PReqVO), "P2P");
    }

    @Override
    public void sendGroup(String msg) throws Exception {
        recordSendLog(sendGroupAsync(msg), "GROUP");
    }

    private void recordSendLog(CompletableFuture<Void> future, String msgWay) {
        future.orTimeout(10, TimeUnit.SECONDS)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        log.info("{} message task completed successfully", msgWay);
                    } else if (throwable instanceof TimeoutException) {
                        log.error("{} message processing timeout", msgWay, throwable);
                    } else {
                        log.warn("{} message task completed with exception", msgWay, throwable);
                    }
                });
    }

    /**
     * 【P2P 发送入口】不走 Netty 上行，经 RouteManager → Route /p2pRoute → 目标 Server 推送。
     * 失败：10s 超时打日志；对方离线 future 异常（见 RouteManager.sendP2P）。
     */
    @Override
    public CompletableFuture<Void> sendP2PAsync(P2PReqVO p2PReqVO) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        p2PReqVO.setUserId(this.conf.getAuth().getUserId());
        return routeManager.sendP2P(future, p2PReqVO);
    }

    /**
     * 【群聊发送入口】RouteManager → Route /groupRoute → 遍历在线用户推送。
     */
    @Override
    public CompletableFuture<Void> sendGroupAsync(String msg) {
        // TODO: 2024/9/12 return messageId
        return this.routeManager.sendGroupMsg(new ChatReqVO(this.conf.getAuth().getUserId(), msg, null));
    }

    @Override
    public ClientConfigurationData.Auth getAuth() {
        return this.conf.getAuth();
    }

    @Override
    public ClientState.State getState() {
        return super.getState();
    }

    @Override
    public Set<CIMUserInfo> getOnlineUser() throws Exception {
        return routeManager.onlineUser();
    }

    @Override
    public Optional<CIMServerResVO> getServerInfo() {
        return Optional.ofNullable(this.serverInfo);
    }
}
