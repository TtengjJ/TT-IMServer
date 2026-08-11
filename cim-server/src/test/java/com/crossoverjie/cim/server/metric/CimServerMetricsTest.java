package com.crossoverjie.cim.server.metric;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Lightweight assertion that push / kick counters increment.
 */
public class CimServerMetricsTest {

    @Test
    public void pushAndKickCountersIncrement() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CimServerMetrics metrics = new CimServerMetrics(registry);

        metrics.pushSuccess();
        metrics.pushOffline();
        metrics.heartbeatKick();

        Assertions.assertEquals(1.0, registry.find("cim.server.push").tag("result", "success").counter().count(), 0.0);
        Assertions.assertEquals(1.0, registry.find("cim.server.push").tag("result", "offline").counter().count(), 0.0);
        Assertions.assertEquals(1.0, registry.find("cim.server.heartbeat.kick").counter().count(), 0.0);
    }
}
