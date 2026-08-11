package com.crossoverjie.cim.server.util;

import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本机在线会话表：userId ↔ Netty Channel（及 userName）。
 * <p>
 * 【作用】消息发送最后一跳 CIMServer.sendMsg 用 userId 找 Channel；
 * 登录 LOGIN_REQUEST 时 put；下线/踢人时 remove。
 * <p>
 * 入口写入：CIMServerHandle.channelRead0(LOGIN_REQUEST)
 * 入口读取：CIMServer.sendMsg / getUserId(channel)
 * 中间件：进程内 ConcurrentHashMap（非 Redis；跨 Server 路由靠 Route 侧 Redis）
 * 失败：get 不到 Channel → 视为本地离线，推送放弃；getUserId 找不到返回 null。
 *
 * @author crossoverJie
 *         Date: 22/05/2018 18:33
 * @since JDK 1.8
 */
public class SessionSocketHolder {
    /** userId → 长连接 Channel */
    private static final Map<Long, NioSocketChannel> CHANNEL_MAP = new ConcurrentHashMap<>(16);
    /** userId → userName */
    private static final Map<Long, String> SESSION_MAP = new ConcurrentHashMap<>(16);

    public static void saveSession(Long userId, String userName) {
        SESSION_MAP.put(userId, userName);
    }

    public static void removeSession(Long userId) {
        SESSION_MAP.remove(userId);
    }

    /**
     * 保存 userId 与 Channel 的绑定关系（登录成功后调用）。
     */
    public static void put(Long id, NioSocketChannel socketChannel) {
        CHANNEL_MAP.put(id, socketChannel);
    }

    /**
     * 按 userId 取 Channel；推送时找不到说明用户不在本机或已断线。
     */
    public static NioSocketChannel get(Long id) {
        return CHANNEL_MAP.get(id);
    }

    public static Map<Long, NioSocketChannel> getRelationShip() {
        return CHANNEL_MAP;
    }

    /**
     * 【可观测性·在线数】供 Micrometer Gauge cim.server.connections 回调。
     * 调用链：MetricsConfig 注册 Gauge → Actuator scrape → 本方法读 CHANNEL_MAP.size()。
     * 写入侧：LOGIN_REQUEST put / 下线 remove，无需在此手动 ±1。
     */
    public static int onlineCount() {
        return CHANNEL_MAP.size();
    }

    /**
     * 按 Channel 反查并移除绑定（下线清理）。
     */
    public static void remove(NioSocketChannel nioSocketChannel) {
        CHANNEL_MAP.entrySet().stream().filter(entry -> entry.getValue() == nioSocketChannel).forEach(entry -> CHANNEL_MAP.remove(entry.getKey()));
    }

    /**
     * 按 Channel 反查 CIMUserInfo；找不到返回 null（可能已清理）。
     */
    public static CIMUserInfo getUserId(NioSocketChannel nioSocketChannel) {
        for (Map.Entry<Long, NioSocketChannel> entry : CHANNEL_MAP.entrySet()) {
            NioSocketChannel value = entry.getValue();
            if (nioSocketChannel == value) {
                Long key = entry.getKey();
                String userName = SESSION_MAP.get(key);
                CIMUserInfo info = new CIMUserInfo(key, userName);
                return info;
            }
        }

        return null;
    }



}
