package com.crossoverjie.cim.server.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * cim-server 业务指标封装（Micrometer Counter）。
 * <p>
 * 【推送计数 cim.server.push】
 * 入口：CIMServer.sendMsg
 * 步骤：Channel==null → result=offline；writeAndFlush 发起 → result=success
 * 观测：GET /actuator/metrics/cim.server.push 、/actuator/prometheus
 * <p>
 * 【心跳踢人 cim.server.heartbeat.kick】
 * 入口：ServerHeartBeatHandlerImpl.process（确认超时后、userOffLine 前）
 * 步骤：READER_IDLE → 超时判定 → +1 → RouteHandler.userOffLine → close
 * 观测：GET /actuator/metrics/cim.server.heartbeat.kick
 * <p>
 * 说明：在线连接数 Gauge 见 MetricsConfig（cim.server.connections）。
 */
@Component
public class CimServerMetrics {

    private static final String TAG_RESULT = "result";
    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_OFFLINE = "offline";

    private final Counter pushSuccess;
    private final Counter pushOffline;
    private final Counter heartbeatKick;

    public CimServerMetrics(MeterRegistry registry) {
        this.pushSuccess = Counter.builder("cim.server.push")
                .description("Server push attempts")
                .tag(TAG_RESULT, RESULT_SUCCESS)
                .register(registry);
        this.pushOffline = Counter.builder("cim.server.push")
                .description("Server push attempts")
                .tag(TAG_RESULT, RESULT_OFFLINE)
                .register(registry);
        this.heartbeatKick = Counter.builder("cim.server.heartbeat.kick")
                .description("Heartbeat timeout kicks")
                .register(registry);
    }

    /** Channel 已找到并发起 writeAndFlush。 */
    public void pushSuccess() {
        pushSuccess.increment();
    }

    /** SessionSocketHolder 无 Channel，本地离线。 */
    public void pushOffline() {
        pushOffline.increment();
    }

    /** 心跳超时即将踢人。 */
    public void heartbeatKick() {
        heartbeatKick.increment();
    }
}
