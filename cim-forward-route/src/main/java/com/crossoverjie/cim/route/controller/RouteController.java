package com.crossoverjie.cim.route.controller;

import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.common.metastore.MetaStore;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.pojo.RouteInfo;
import com.crossoverjie.cim.common.res.BaseResponse;
import com.crossoverjie.cim.common.res.NULLBody;
import com.crossoverjie.cim.common.route.algorithm.RouteHandle;
import com.crossoverjie.cim.common.util.RouteInfoParseUtil;
import com.crossoverjie.cim.route.api.RouteApi;
import com.crossoverjie.cim.route.api.vo.req.AckMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.req.LoginReqVO;
import com.crossoverjie.cim.route.api.vo.req.OfflineMsgReqVO;
import com.crossoverjie.cim.route.api.vo.req.P2PReqVO;
import com.crossoverjie.cim.route.api.vo.req.RegisterInfoReqVO;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.api.vo.res.RegisterInfoResVO;
import com.crossoverjie.cim.route.api.vo.res.SendMsgResVO;
import com.crossoverjie.cim.route.service.AccountService;
import com.crossoverjie.cim.route.service.CommonBizService;
import com.crossoverjie.cim.route.service.OfflineMsgService;
import com.crossoverjie.cim.route.service.UserInfoCacheService;
import com.crossoverjie.cim.server.api.ServerApi;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import static com.crossoverjie.cim.common.enums.StatusEnum.OFF_LINE;

/**
 * Route 控制面 HTTP 入口（实现 RouteApi）。
 * <p>
 * 【总览】Client/Server 通过 HTTP 调用本类；长连接推送在 cim-server。
 * 中间件：Spring MVC、Redis（账号/路由）、Zookeeper MetaStore（可用 Server）、
 * OkHttp/RpcProxy（push 时调 ServerApi.sendMsg）。
 * <p>
 * 主要链路：
 * <ul>
 *   <li>login —— 鉴权 + 选服 + 写路由</li>
 *   <li>p2pRoute / groupRoute —— 查路由后 HTTP 推到目标 Server</li>
 *   <li>offLine —— 清 Redis 登录态与路由</li>
 * </ul>
 *
 * @author crossoverJie
 * Date: 22/05/2018 14:46
 * @since JDK 1.8
 */
@Slf4j
@Controller
@RequestMapping("/")
public class RouteController implements RouteApi {

    @Resource
    private MetaStore metaStore;

    @Resource
    private AccountService accountService;

    @Resource
    private UserInfoCacheService userInfoCacheService;

    @Resource
    private CommonBizService commonBizService;

    @Resource
    private RouteHandle routeHandle;

    @Resource
    private ServerApi serverApi;

    @Resource
    private OfflineMsgService offlineMsgService;


    /**
     * 【群聊发送链路】
     * 入口：本方法 POST /groupRoute
     * 步骤：1. Redis SCAN 加载全部在线路由 2. 跳过发送者 3. 对每个在线用户 pushMsg → Server /sendMsg → Netty
     * 中间件：Redis（ROUTE_PREFIX）、HTTP→cim-server、Netty
     * 失败：某用户 Channel 已断时 Server 侧仅打日志返回；本接口仍返回 SUCCESS（尽力推送）
     */
    @Operation(summary = "群聊 API")
    @RequestMapping(value = "groupRoute", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<NULLBody> groupRoute(@RequestBody ChatReqVO groupReqVO) {
        BaseResponse<NULLBody> res = new BaseResponse();

        log.info("msg=[{}]", groupReqVO.toString());

        Map<Long, CIMServerResVO> serverResVoMap = accountService.loadRouteRelated();
        for (Map.Entry<Long, CIMServerResVO> cimServerResVoEntry : serverResVoMap.entrySet()) {
            Long userId = cimServerResVoEntry.getKey();
            CIMServerResVO cimServerResVO = cimServerResVoEntry.getValue();
            if (userId.equals(groupReqVO.getUserId())) {
                // Skip the sender
                Optional<CIMUserInfo> cimUserInfo = userInfoCacheService.loadUserInfoByUserId(groupReqVO.getUserId());
                cimUserInfo.ifPresent(userInfo -> log.warn("skip send user userId={}", userInfo));
                continue;
            }

            // Push message
            ChatReqVO chatVO = new ChatReqVO(userId, groupReqVO.getMsg(), null);
            accountService.pushMsg(cimServerResVO, groupReqVO.getUserId(), chatVO);

        }

        res.setCode(StatusEnum.SUCCESS.getCode());
        res.setMessage(StatusEnum.SUCCESS.getMessage());
        return res;
    }


    /**
     * 【P2P 发送链路】
     * 入口：POST /p2pRoute
     * 步骤：1. 落 Outbox PENDING（在线离线统一）2. 无路由则保持 PENDING 返回 OFF_LINE+messageId
     *       3. 有路由则 pushMsg（properties 含 messageId）→ afterPushed(SENT+Wheel)
     * 失败：发送方不存在则 save 返回 null
     */
    @Operation(summary = "私聊 API")
    @RequestMapping(value = "p2pRoute", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<SendMsgResVO> p2pRoute(@RequestBody P2PReqVO p2pRequest) {
        BaseResponse<SendMsgResVO> res = new BaseResponse<>();
        try {
            OfflineMsg offlineMsg = offlineMsgService.saveOfflineMsg(p2pRequest);
            if (offlineMsg == null) {
                res.setCode(StatusEnum.FAIL.getCode());
                res.setMessage("save delivery record failed");
                return res;
            }
            Long messageId = offlineMsg.getMessageId();
            SendMsgResVO body = SendMsgResVO.builder().messageId(messageId).msg("success").build();

            Optional<CIMServerResVO> cimServerResVO =
                    accountService.loadRouteRelatedByUserId(p2pRequest.getReceiveUserId());
            if (cimServerResVO.isEmpty()) {
                log.warn("userId={} not online, keep PENDING messageId={}",
                        p2pRequest.getReceiveUserId(), messageId);
                res.setCode(OFF_LINE.getCode());
                res.setMessage(OFF_LINE.getMessage());
                res.setDataBody(body);
                return res;
            }

            ChatReqVO chatVO = new ChatReqVO(p2pRequest.getReceiveUserId(), p2pRequest.getMsg(),
                    p2pRequest.getBatchMsg());
            chatVO.setMessageId(messageId);
            accountService.pushMsg(cimServerResVO.get(), p2pRequest.getUserId(), chatVO);
            offlineMsgService.afterPushed(p2pRequest.getReceiveUserId(), messageId);

            res.setCode(StatusEnum.SUCCESS.getCode());
            res.setMessage(StatusEnum.SUCCESS.getMessage());
            res.setDataBody(body);
        } catch (CIMException e) {
            res.setCode(e.getErrorCode());
            res.setMessage(e.getErrorMessage());
        }
        return res;
    }


    /**
     * 【ACK】POST /ackMsg → markAcked + Wheel.cancel
     */
    @Operation(summary = "消息 ACK")
    @RequestMapping(value = "ackMsg", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<NULLBody> ackMsg(@RequestBody AckMsgReqVO ackMsgReqVO) {
        BaseResponse<NULLBody> res = new BaseResponse<>();
        offlineMsgService.ackMsg(ackMsgReqVO.getReceiveUserId(), ackMsgReqVO.getMessageId());
        res.setCode(StatusEnum.SUCCESS.getCode());
        res.setMessage(StatusEnum.SUCCESS.getMessage());
        return res;
    }

    /**
     * 【下线链路】
     * 入口：本方法 POST /offLine（Client.close / Server 踢人 / 重连前清理均可调用）
     * 步骤：1. 查用户缓存 2. accountService.offLine → Redis Lua 删路由 + 登录态
     * 中间件：Redis（ROUTE_PREFIX、login-status）
     * 失败：用户信息不存在则跳过清理，仍返回 SUCCESS（幂等友好）
     */
    @Operation(summary = "客户端下线")
    @RequestMapping(value = "offLine", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<NULLBody> offLine(@RequestBody ChatReqVO chatReqVO) {
        BaseResponse<NULLBody> res = new BaseResponse();

        Optional<CIMUserInfo> cimUserInfo = userInfoCacheService.loadUserInfoByUserId(chatReqVO.getUserId());

        cimUserInfo.ifPresent(userInfo -> {
            log.info("user [{}] offline!", userInfo);
            accountService.offLine(chatReqVO.getUserId());
        });

        res.setCode(StatusEnum.SUCCESS.getCode());
        res.setMessage(StatusEnum.SUCCESS.getMessage());
        return res;
    }

    /**
     * 【登录选服链路】
     * 入口：本方法 POST /login
     * 步骤：1. Redis 校验账号并写登录态 2. MetaStore(ZK) 取可用 Server
     *       3. RouteHandle 按 userId 选服 4. 可用性探测 5. Redis 写 ROUTE_PREFIX+userId
     * 中间件：Redis、Zookeeper、RouteHandle（一致性哈希/轮询/随机）
     * 失败：账号不匹配 / 重复登录 → 直接返回错误码，不选服；
     *       无可用 Server / 探测失败 → 由 checkServerAvailable / routeHandle 抛异常向上传递
     */
    @Operation(summary = "登录并获取服务器")
    @RequestMapping(value = "login", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<CIMServerResVO> login(@RequestBody LoginReqVO loginReqVO) throws Exception {
        BaseResponse<CIMServerResVO> res = new BaseResponse();
        //登录校验
        StatusEnum status = accountService.login(loginReqVO);
        res.setCode(status.getCode());
        res.setMessage(status.getMessage());
        if (status != StatusEnum.SUCCESS) {
            return res;
        }

        // check server available
        Set<String> availableServerList = metaStore.getAvailableServerList();
        String key = String.valueOf(loginReqVO.getUserId());
        String server =
                routeHandle.routeServer(List.copyOf(availableServerList), key);
        log.info("userInfo=[{}] route server info=[{}]", loginReqVO, server);

        RouteInfo routeInfo = RouteInfoParseUtil.parse(server);
        routeInfo = commonBizService.checkServerAvailable(routeInfo, key);

        //保存路由信息
        accountService.saveRouteInfo(loginReqVO, server);

        CIMServerResVO vo =
                new CIMServerResVO(routeInfo.getIp(), routeInfo.getCimServerPort(), routeInfo.getHttpPort());
        res.setDataBody(vo);
        return res;
    }

    /**
     * 注册账号
     *
     * @return
     */
    @Operation(summary = "注册账号")
    @RequestMapping(value = "registerAccount", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<RegisterInfoResVO> registerAccount(@RequestBody RegisterInfoReqVO registerInfoReqVO)
            throws Exception {
        BaseResponse<RegisterInfoResVO> res = new BaseResponse();

        long userId = System.currentTimeMillis();
        RegisterInfoResVO info = new RegisterInfoResVO(userId, registerInfoReqVO.getUserName());
        info = accountService.register(info);

        res.setDataBody(info);
        res.setCode(StatusEnum.SUCCESS.getCode());
        res.setMessage(StatusEnum.SUCCESS.getMessage());
        return res;
    }

    /**
     * 获取所有在线用户
     *
     * @return
     */
    @Operation(summary = "获取所有在线用户")
    @RequestMapping(value = "onlineUser", method = RequestMethod.GET)
    @ResponseBody()
    @Override
    public BaseResponse<Set<CIMUserInfo>> onlineUser() throws Exception {
        BaseResponse<Set<CIMUserInfo>> res = new BaseResponse();

        Set<CIMUserInfo> cimUserInfos = userInfoCacheService.onlineUser();
        res.setDataBody(cimUserInfos);
        res.setCode(StatusEnum.SUCCESS.getCode());
        res.setMessage(StatusEnum.SUCCESS.getMessage());
        return res;
    }

    @Operation(summary = "Client fetch offline messages")
    @RequestMapping(value = "fetchOfflineMsgs", method = RequestMethod.POST)
    @ResponseBody()
    @Override
    public BaseResponse<NULLBody> fetchOfflineMsgs(@RequestBody OfflineMsgReqVO offlineMsgReqVO) {
        BaseResponse<NULLBody> res = new BaseResponse();

        try {
            // Get the routing information of the user receiving the message
            Optional<CIMServerResVO> cimServerResVO = accountService.loadRouteRelatedByUserId(offlineMsgReqVO.getReceiveUserId());

            cimServerResVO.ifPresent(cimServerRes -> {
                offlineMsgService.fetchOfflineMsgs(cimServerRes, offlineMsgReqVO.getReceiveUserId());
            });

            res.setCode(StatusEnum.SUCCESS.getCode());
            res.setMessage(StatusEnum.SUCCESS.getMessage());

        } catch (CIMException e) {
            res.setCode(e.getErrorCode());
            res.setMessage(e.getErrorMessage());
        }
        return res;
    }
}
