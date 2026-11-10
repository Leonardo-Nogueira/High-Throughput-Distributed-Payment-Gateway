package com.leonardonogueira.campeao.worker;

import com.leonardonogueira.campeao.client.GatewayClient;
import com.leonardonogueira.campeao.config.CampeaoConfig;
import com.leonardonogueira.campeao.model.CampeaoModels.GatewayHealth;
import com.leonardonogueira.campeao.storage.RedisStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Equivalente 1:1 ao 'healthCheck.ts' do projeto vencedor da Rinha 2025.
 * 
 * Monitora continuamente a saúde dos processadores a cada 5.1s (respeitando
 * estritamente o rate limit de 5s para evitar HTTP 429) e grava o estado
 * no Redis para que todos os workers e instâncias tomem decisões alinhadas.
 */
@Component
public class HealthCheckWorker {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckWorker.class);

    private final GatewayClient client;
    private final RedisStorage storage;
    private final CampeaoConfig config;

    public HealthCheckWorker(GatewayClient client, RedisStorage storage, CampeaoConfig config) {
        this.client = client;
        this.storage = storage;
        this.config = config;
    }

    @Scheduled(fixedDelay = 5100)
    public void monitorGateways() {
        try {
            GatewayHealth defaultHealth = client.checkHealth(config.getDefaultGatewayUrl());

            if (defaultHealth.statusCode() == 200) {
                boolean isHealthy = !defaultHealth.failing();
                storage.setGatewayHealthy("default", isHealthy);

                if (isHealthy) {
                    log.debug("💚 [HEALTH CHECK] Gateway DEFAULT saudável (minResponseTime: {}ms)", defaultHealth.minResponseTime());
                } else {
                    log.warn("⚠️ [HEALTH CHECK] Gateway DEFAULT reportou failing: true! Desviando para FALLBACK.");
                }
            } else if (defaultHealth.statusCode() == 429) {
                log.error("🚨 [HEALTH CHECK] VIOLAÇÃO DE RATE LIMIT: Recebido HTTP 429 do Default!");
            }
        } catch (Exception e) {
            log.warn("Erro ao verificar saúde do processador Default: {}", e.getMessage());
        }
    }
}

