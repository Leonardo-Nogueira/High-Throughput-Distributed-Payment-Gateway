package com.leonardonogueira.campeao.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Equivalente 1:1 ao 'config.ts' do projeto vencedor da Rinha 2025.
 * Centraliza variáveis de ambiente, portas, URLs de gateways e limites.
 */
@Configuration
public class CampeaoConfig {

    @Value("${campeao.redis.host:localhost}")
    private String redisHost;

    @Value("${campeao.redis.port:6379}")
    private int redisPort;

    @Value("${campeao.gateway.default-url:http://localhost:9999/mock/default}")
    private String defaultGatewayUrl;

    @Value("${campeao.gateway.fallback-url:http://localhost:9999/mock/fallback}")
    private String fallbackGatewayUrl;

    @Value("${campeao.gateway.timeout-ms:250}")
    private int gatewayTimeoutMs;

    @Value("${campeao.worker.concurrency:8}")
    private int workerConcurrency;

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public String getDefaultGatewayUrl() {
        return defaultGatewayUrl;
    }

    public String getFallbackGatewayUrl() {
        return fallbackGatewayUrl;
    }

    public int getGatewayTimeoutMs() {
        return gatewayTimeoutMs;
    }

    public int getWorkerConcurrency() {
        return workerConcurrency;
    }
}

