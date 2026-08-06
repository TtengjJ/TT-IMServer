package com.crossoverjie.cim.route.service.impl;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.pojo.CIMUserInfo;
import com.crossoverjie.cim.common.pojo.RouteInfo;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.util.RouteInfoParseUtil;
import com.crossoverjie.cim.route.api.vo.req.ChatReqVO;
import com.crossoverjie.cim.route.api.vo.req.LoginReqVO;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.api.vo.res.RegisterInfoResVO;
import com.crossoverjie.cim.route.service.AccountService;
import com.crossoverjie.cim.route.service.UserInfoCacheService;
import com.crossoverjie.cim.server.api.ServerApi;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.crossoverjie.cim.route.constant.Constant.ACCOUNT_PREFIX;
import static com.crossoverjie.cim.route.constant.Constant.LOGIN_STATUS_PREFIX;
import static com.crossoverjie.cim.route.constant.Constant.ROUTE_PREFIX;

/**
 * 账号 / 路由 / 推送 的 Redis 实现（被 RouteController 调用）。
 * <p>
 * Key 约定：
 * <ul>
 *   <li>ACCOUNT_PREFIX+userId → userName（账号）</li>
 *   <li>login-status → 在线用户集合</li>
 *   <li>ROUTE_PREFIX+userId → "ip:cimPort:httpPort"（消息路由）</li>
 * </ul>
 * 中间件：Redis、ServerApi（HTTP 调目标 cim-server）。
 *
 * @author crossoverJie
 * Date: 2018/12/23 21:58
 * @since JDK 1.8
 */
@Slf4j
@Service
public class AccountServiceRedisImpl implements AccountService {

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Resource
    private UserInfoCacheService userInfoCacheService;

    @Resource
    private ServerApi serverApi;

    @Override
    public RegisterInfoResVO register(RegisterInfoResVO info) {
        String key = ACCOUNT_PREFIX + info.getUserId();

        String name = redisTemplate.opsForValue().get(info.getUserName());
        if (null == name) {
            //为了方便查询，冗余一份
            redisTemplate.opsForValue().set(key, info.getUserName());
            redisTemplate.opsForValue().set(info.getUserName(), key);
        } else {
            long userId = Long.parseLong(name.split(":")[1]);
            info.setUserId(userId);
            info.setUserName(info.getUserName());
        }

        return info;
    }

    /**
     * 【登录鉴权】入口由 RouteController.login 调用。
     * 步骤：查账号 → 校验 userName → 写登录态（防重复登录）。
     * 失败：ACCOUNT_NOT_MATCH / REPEAT_LOGIN，上层不再选服。
     */
    @Override
    public StatusEnum login(LoginReqVO loginReqVO) throws Exception {
        //再去Redis里查询
        String key = ACCOUNT_PREFIX + loginReqVO.getUserId();
        String userName = redisTemplate.opsForValue().get(key);
        if (null == userName) {
            return StatusEnum.ACCOUNT_NOT_MATCH;
        }

        if (!userName.equals(loginReqVO.getUserName())) {
            return StatusEnum.ACCOUNT_NOT_MATCH;
        }

        //登录成功，保存登录状态
        boolean status = userInfoCacheService.saveAndCheckUserLoginStatus(loginReqVO.getUserId());
        if (!status) {
            //重复登录
            return StatusEnum.REPEAT_LOGIN;
        }

        return StatusEnum.SUCCESS;
    }

    /**
     * 【写路由】登录选服成功后调用，后续 P2P/群聊靠此 key 找目标 Server。
     * 失败：Redis 异常向上抛，登录接口失败。
     */
    @Override
    public void saveRouteInfo(LoginReqVO loginReqVO, String msg) throws Exception {
        String key = ROUTE_PREFIX + loginReqVO.getUserId();
        redisTemplate.opsForValue().set(key, msg);
    }

    /**
     * 【群聊路由表】SCAN 所有 ROUTE_PREFIX*，得到 userId → Server 映射。
     * 失败：连接/SCAN 异常向上抛；无在线用户则返回空 Map（群聊空转）。
     */
    @Override
    public Map<Long, CIMServerResVO> loadRouteRelated() {

        Map<Long, CIMServerResVO> routes = new HashMap<>(64);


        RedisConnection connection = redisTemplate.getConnectionFactory().getConnection();
        ScanOptions options = ScanOptions.scanOptions()
                .match(ROUTE_PREFIX + "*")
                .build();
        Cursor<byte[]> scan = connection.scan(options);

        while (scan.hasNext()) {
            byte[] next = scan.next();
            String key = new String(next, StandardCharsets.UTF_8);
            log.info("key={}", key);
            parseServerInfo(routes, key);

        }
        scan.close();

        return routes;
    }

    /**
     * 【P2P 查路由】按 receiveUserId 取所在 Server；empty 表示离线。
     */
    @Override
    public Optional<CIMServerResVO> loadRouteRelatedByUserId(Long userId) {
        String value = redisTemplate.opsForValue().get(ROUTE_PREFIX + userId);

        if (value == null) {
            return Optional.empty();
        }

        RouteInfo parse = RouteInfoParseUtil.parse(value);
        CIMServerResVO cimServerResVO =
                new CIMServerResVO(parse.getIp(), parse.getCimServerPort(), parse.getHttpPort());
        return Optional.of(cimServerResVO);
    }

    private void parseServerInfo(Map<Long, CIMServerResVO> routes, String key) {
        long userId = Long.valueOf(key.split(":")[1]);
        String value = redisTemplate.opsForValue().get(key);
        RouteInfo parse = RouteInfoParseUtil.parse(value);
        CIMServerResVO cimServerResVO =
                new CIMServerResVO(parse.getIp(), parse.getCimServerPort(), parse.getHttpPort());
        routes.put(userId, cimServerResVO);
    }


    /**
     * 【消息推送链路】Route → Server HTTP /sendMsg → Netty 下发。
     * 入口：p2pRoute / groupRoute / 离线补发。
     * 步骤：拼 SendMsgReqVO(cmd=MESSAGE + meta) → ServerApi.sendMsg(url=接收方 Server)。
     * 中间件：HTTP（RpcProxy/OkHttp）、目标机 SessionSocketHolder。
     * 失败：发送方用户缓存缺失则跳过推送；目标 Channel 不在由 Server.sendMsg 打日志返回。
     */
    @Override
    public void pushMsg(CIMServerResVO cimServerResVO, long sendUserId, ChatReqVO chatReqVO) {
        Optional<CIMUserInfo> cimUserInfo = userInfoCacheService.loadUserInfoByUserId(sendUserId);

        cimUserInfo.ifPresent(sendUserInfo -> {
            String url = "http://" + cimServerResVO.getIp() + ":" + cimServerResVO.getHttpPort();
            SendMsgReqVO vo =
                    new SendMsgReqVO(chatReqVO.getMsg(), chatReqVO.getUserId(), chatReqVO.getBatchMsg(), BaseCommand.MESSAGE);
            Map<String, String> properties = new HashMap<>();
            properties.put(Constants.MetaKey.SEND_USER_ID, String.valueOf(sendUserId));
            properties.put(Constants.MetaKey.SEND_USER_NAME, sendUserInfo.getUserName());
            properties.put(Constants.MetaKey.RECEIVE_USER_ID, String.valueOf(chatReqVO.getUserId()));
            if (chatReqVO.getMessageId() != null) {
                properties.put(Constants.MetaKey.MESSAGE_ID, String.valueOf(chatReqVO.getMessageId()));
            }
            vo.setProperties(properties);
            serverApi.sendMsg(vo, url);

        });
    }

    /**
     * 【清路由+登录态】Lua 原子删除 ROUTE_PREFIX 与 login-status 中的 userId。
     * 失败：脚本执行异常向上抛。
     */
    @Override
    public void offLine(Long userId) {

        DefaultRedisScript redisScript = new DefaultRedisScript();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/offLine.lua")));

        redisTemplate.execute(redisScript,
                Collections.singletonList(ROUTE_PREFIX + userId),
                LOGIN_STATUS_PREFIX,
                userId.toString());
    }
}
