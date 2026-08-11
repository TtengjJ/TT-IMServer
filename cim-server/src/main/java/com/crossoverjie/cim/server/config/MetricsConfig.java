package com.crossoverjie.cim.server.config;

import com.crossoverjie.cim.server.util.SessionSocketHolder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Configuration;

/**
 * 【在线连接数 cim.server.connections】
 * 入口：Spring 启动构造本配置 → 注册函数式 Gauge
 * 步骤：Actuator scrape / 查询 metrics 时回调 SessionSocketHolder.onlineCount()
 * 数据源：LOGIN_REQUEST put / 下线 remove 后 CHANNEL_MAP.size() 自动反映，无需手动 inc/dec
 * 观测：GET /actuator/metrics/cim.server.connections 、/actuator/prometheus
 */
@Configuration
public class MetricsConfig {

    public MetricsConfig(MeterRegistry registry) {
        Gauge.builder("cim.server.connections", SessionSocketHolder::onlineCount)
                .description("Active Netty sessions on this server")
                .register(registry);
    }
}
