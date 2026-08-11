package com.crossoverjie.cim.route.metric;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Assert;
import org.junit.Test;

/**
 * Lightweight assertion for login timer and delivery counters.
 */
public class CimRouteMetricsTest {

    @Test
    public void loginAndDeliveryCounters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CimRouteMetrics metrics = new CimRouteMetrics(registry);

        Timer.Sample sample = metrics.startLogin();
        metrics.stopLogin(sample, true);
        metrics.deliveryAck();
        metrics.retryScheduled();
        metrics.retryExecuted();

        Assert.assertEquals(1L, registry.find("cim.route.login").tag("result", "success").timer().count());
        Assert.assertEquals(1.0, registry.find("cim.route.delivery.ack").counter().count(), 0.0);
        Assert.assertEquals(1.0, registry.find("cim.route.delivery.retry").tag("phase", "scheduled").counter().count(), 0.0);
        Assert.assertEquals(1.0, registry.find("cim.route.delivery.retry").tag("phase", "executed").counter().count(), 0.0);
    }
}
