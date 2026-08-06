package com.crossoverjie.cim.route.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "cim.delivery")
public class DeliveryProperties {
    private int ackTimeoutSeconds = 10;
    private int maxRetry = 3;
    private int reloadLimit = 500;
}
