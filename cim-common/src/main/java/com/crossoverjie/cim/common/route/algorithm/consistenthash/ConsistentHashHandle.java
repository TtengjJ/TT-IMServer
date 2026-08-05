package com.crossoverjie.cim.common.route.algorithm.consistenthash;

import com.crossoverjie.cim.common.pojo.RouteInfo;
import com.crossoverjie.cim.common.route.algorithm.RouteHandle;

import com.crossoverjie.cim.common.util.RouteInfoParseUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 一致性哈希选服：委托 AbstractConsistentHash（TreeMap / SortArrayMap 等）。
 * <p>
 * 入口：RouteController.login → routeServer
 * 步骤：process 清空环 → 将各 Server hash 入环 → 按 key 顺时针取最近节点
 * 失败：无节点时由底层 getFirstNodeValue 行为决定；扩缩容时仅影响相邻虚拟区间。
 *
 * @author crossoverJie
 * Date: 2019-02-27 00:33
 * @since JDK 1.8
 */
public class ConsistentHashHandle implements RouteHandle {
    private AbstractConsistentHash hash;

    public void setHash(AbstractConsistentHash hash) {
        this.hash = hash;
    }

    @Override
    public String routeServer(List<String> values, String key) {
        return hash.process(values, key);
    }

    @Override
    public List<String> removeExpireServer(RouteInfo routeInfo) {
        Map<String, String> remove = hash.remove(RouteInfoParseUtil.parse(routeInfo));
        return new ArrayList<>(remove.keySet());
    }
}
