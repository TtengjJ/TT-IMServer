package com.crossoverjie.cim.route.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * cim-forward-route 业务指标封装（Micrometer Timer / Counter）。
 * <p>
 * 【登录耗时 cim.route.login】
 * 入口：RouteController.login
 * 步骤：Sample.start → 鉴权+选服+写路由 → finally stop(tag result=success|fail)
 * 观测：GET /actuator/metrics/cim.route.login
 * <p>
 * 【ACK 确认 cim.route.delivery.ack】
 * 入口：OfflineMsgServiceImpl.ackMsg（Client MESSAGE_ACK → Server → Route /ackMsg）
 * 步骤：markAcked + Wheel.cancel 后 +1
 * 观测：GET /actuator/metrics/cim.route.delivery.ack
 * <p>
 * 【超时重推 cim.route.delivery.retry】
 * 入口：DeliveryRetryManager.scheduleAfterSent → phase=scheduled；
 *       RetryTask.run → phase=executed
 * 观测：GET /actuator/metrics/cim.route.delivery.retry
 * 解读：retry 升高而 ack 偏低 → 客户端未确认或 ACK 超时偏紧
 */
@Component
public class CimRouteMetrics {

    private static final String TAG_RESULT = "result";
    private static final String TAG_PHASE = "phase";
    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_FAIL = "fail";
    private static final String PHASE_SCHEDULED = "scheduled";
    private static final String PHASE_EXECUTED = "executed";

    private final MeterRegistry registry;
    private final Counter deliveryAck;
    private final Counter retryScheduled;
    private final Counter retryExecuted;

    public CimRouteMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.deliveryAck = Counter.builder("cim.route.delivery.ack")
                .description("Delivery ACK confirmations")
                .register(registry);
        this.retryScheduled = Counter.builder("cim.route.delivery.retry")
                .description("Delivery retry schedule/execute counts")
                .tag(TAG_PHASE, PHASE_SCHEDULED)
                .register(registry);
        this.retryExecuted = Counter.builder("cim.route.delivery.retry")
                .description("Delivery retry schedule/execute counts")
                .tag(TAG_PHASE, PHASE_EXECUTED)
                .register(registry);
    }

    /** 登录链路计时起点（HTTP 线程，勿进 EventLoop）。 */
    public Timer.Sample startLogin() {
        return Timer.start(registry);
    }

    /** 登录结束落盘；fail=鉴权失败/重复登录/选服异常等非成功路径。 */
    public void stopLogin(Timer.Sample sample, boolean success) {
        sample.stop(Timer.builder("cim.route.login")
                .description("Login auth + server selection latency")
                .tag(TAG_RESULT, success ? RESULT_SUCCESS : RESULT_FAIL)
                .register(registry));
    }

    /** Outbox ACKED 成功后计数。 */
    public void deliveryAck() {
        deliveryAck.increment();
    }

    /** 已向时间轮登记一次重推任务。 */
    public void retryScheduled() {
        retryScheduled.increment();
    }

    /** 时间轮触发 RetryTask 实际执行。 */
    public void retryExecuted() {
        retryExecuted.increment();
    }
}
