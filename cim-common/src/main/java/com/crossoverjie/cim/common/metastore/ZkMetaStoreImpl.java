package com.crossoverjie.cim.common.metastore;

import com.crossoverjie.cim.common.pojo.RouteInfo;
import com.crossoverjie.cim.common.util.RouteInfoParseUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

/**
 * Zookeeper 实现的 MetaStore：IM-Server 注册与发现。
 * <p>
 * 【注册发现链路】
 * 注册入口：RegistryMetaStore.run → addServer（临时节点 /cim/ip:cimPort:httpPort）
 * 发现入口：RouteController.login → getAvailableServerList（Caffeine 缓存 + ZK children）
 * 变更：listenServerList 订阅子节点变化 → rebuildCache
 * 中间件：Zookeeper(ZkClient)、Caffeine 本地缓存
 * 失败：ZK 不可达 initialize/addServer 抛异常，Server 注册失败或 Route 选服失败；
 *       缓存为空时回源 ZK，仍空则登录选服列表为空。
 *
 * @author crossovreJie
 */
@Slf4j
public class ZkMetaStoreImpl implements MetaStore {
    public static final String ROOT = "/cim";

    private ZkClient client;

    Cache<String, String> cache;

    @Override
    public void initialize(AbstractConfiguration<?> configuration) throws Exception {
        cache = Caffeine.newBuilder().build();
        client = new ZkClient(configuration.getMetaServiceUri(), configuration.getTimeoutMs());
    }

    /**
     * 获取可用 Server 列表（优先本地缓存，未命中则读 ZK /cim 子节点）。
     * 失败：ZK 异常向上抛；无节点返回空集合，后续 routeServer 可能 SERVER_NOT_AVAILABLE。
     */
    @Override
    public Set<String> getAvailableServerList() throws Exception {
        if (cache.asMap().size() > 0) {
            return cache.asMap().keySet();
        }
        List<String> coll = client.getChildren(ROOT);
        Map<String, String> voidMap = coll.stream().collect(Collectors.toMap(
                Function.identity(),
                Function.identity()
        ));
        cache.putAll(voidMap);
        return voidMap.keySet();
    }

    /**
     * Server 启动注册：持久化根节点 /cim，再创建临时子节点（进程挂掉自动消失）。
     * 失败：ZK 写失败抛异常，该 Server 不会进入选服列表。
     */
    @Override
    public void addServer(String ip, int cimServerPort, int httpPort) throws Exception {
        boolean exists = client.exists(ROOT);
        if (!exists) {
            client.createPersistent(ROOT);
        }
        String zkParse = RouteInfoParseUtil.parse(RouteInfo.builder()
                .ip(ip)
                .cimServerPort(cimServerPort)
                .httpPort(httpPort)
                .build());
        String serverPath = String.format("%s/%s", ROOT, zkParse);
        client.createEphemeral(serverPath);
        log.info("Add server to zk [{}]", serverPath);
    }

    /**
     * Route 侧订阅 Server 列表变更；变更后刷新本地缓存。
     */
    @Override
    public void listenServerList(ChildListener childListener) throws Exception {
        client.subscribeChildChanges(ROOT, (parentPath, currentChildren) -> {
            log.info("Clear and update local cache parentPath=[{}],current server list=[{}]", parentPath, currentChildren.toString());
            childListener.childChanged(parentPath, currentChildren);

            // TODO: 2024/8/19 maybe can reuse currentChildren.
            // Because rebuildCache() will re-fetch the server list from zk.
            rebuildCache();
        });
    }

    @Override
    public synchronized void rebuildCache() throws Exception {
        cache.invalidateAll();
        this.getAvailableServerList();

    }


    private List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception {
        /**
         * Get children and set a watcher on the node. The watcher notification will come through the
         * CuratorListener (see setDataAsync() above).
         */
        return client.getChildren().watched().forPath(path);
    }

    private void createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception {
        // this will create the given EPHEMERAL ZNode with the given data
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    private void create(CuratorFramework client, String path, byte[] payload) throws Exception {
        // this will create the given ZNode with the given data
        client.create().forPath(path, payload);
    }

    private void watchedGetChildren(CuratorFramework client, String path, Watcher watcher)
            throws Exception {
        // Get children and set the given watcher on the node.
        client.getChildren().usingWatcher(watcher).forPath(path);
    }
}
