package com.crossoverjie.cim.server.kit;

import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.route.api.RouteApi;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.server.util.SessionSocketHolder;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Server 侧用户下线协调：清本机会话并通知 Route。
 * <p>
 * 【下线/踢人链路】入口：CIMServerHandle.channelInactive 或 ServerHeartBeatHandlerImpl 超时
 * 步骤：removeSession + remove(channel) → HTTP RouteApi.offLine → Redis 清路由
 * 中间件：SessionSocketHolder、HTTP→Route、Redis
 * 失败：userInfo 为 null 时只 remove channel；Route 调用失败影响 Redis 状态一致性。
 *
 * @author crossoverJie
 * Date: 2019-01-20 17:20
 * @since JDK 1.8
 */
@Component
@Slf4j
public class RouteHandler {

    @Resource
    private RouteApi routeApi;

    /**
     * 用户下线
     *
     * @param userInfo
     * @param channel
     */
    public void userOffLine(CIMUserInfo userInfo, NioSocketChannel channel) {
        if (userInfo != null) {
            log.info("Account [{}] offline", userInfo.getUserName());
            SessionSocketHolder.removeSession(userInfo.getUserId());
            //清除路由关系
            clearRouteInfo(userInfo);
        }
        SessionSocketHolder.remove(channel);

    }


    /**
     * 清除路由关系
     *
     * @param userInfo
     * @throws IOException
     */
    public void clearRouteInfo(CIMUserInfo userInfo) {
        ChatReqVO vo = new ChatReqVO(userInfo.getUserId(), userInfo.getUserName(), null);
        routeApi.offLine(vo);
    }

}
