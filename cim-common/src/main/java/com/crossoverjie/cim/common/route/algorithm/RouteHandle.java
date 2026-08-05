package com.crossoverjie.cim.common.route.algorithm;

import com.crossoverjie.cim.common.pojo.RouteInfo;
import java.util.List;

/**
 * 登录选服算法接口。
 * <p>
 * 【选服链路】入口：RouteController.login → routeServer(availableServers, userId)
 * 实现：ConsistentHashHandle（默认）/ LoopHandle / RandomHandle
 * 中间件：无外部中间件，纯内存算法；Server 列表来自 MetaStore(ZK)
 * 失败：列表为空时 Loop/Random 抛 SERVER_NOT_AVAILABLE；一致性哈希依赖具体实现。
 *
 * @author crossoverJie
 * Date: 2019-02-27 00:31
 * @since JDK 1.8
 */
public interface RouteHandle {

    /**
     * 在一批 Server（形如 ip:cimPort:httpPort）中为 key（通常 userId）选一台。
     * @param values 可用 Server 列表
     * @param key 路由键，一般为 userId 字符串
     * @return 选中的 Server 描述串
     */
    // TODO: 2024/9/13 Use List<RouteInfo> instead of List<String> to make the code more type-safe
    String routeServer(List<String> values, String key);

    /**
     * 剔除不可用节点后返回剩余列表（可用性探测失败时使用）。
     */
    List<String> removeExpireServer(RouteInfo routeInfo);
}
