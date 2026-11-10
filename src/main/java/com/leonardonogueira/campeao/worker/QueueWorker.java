package com.leonardonogueira.campeao.worker;

import com.leonardonogueira.campeao.client.GatewayClient;
import com.leonardonogueira.campeao.config.CampeaoConfig;
import com.leonardonogueira.campeao.model.CampeaoModels.*;
import com.leonardonogueira.campeao.storage.RedisStorage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Equivalente 1:1 ao 'queueWorker.ts' do projeto vencedor da Rinha 2025.
 * 
 * Este worker roda continuamente em segundo plano consumindo pagamentos enfileirados
 * no Redis/Memória. Ele executa:
 * 1. Drenagem da fila de forma concorrente em Virtual Threads.
 * 2. Consulta ao estado de saúde do processador DEFAULT no Redis.
 * 3. Envio para o DEFAULT (menor taxa). Se o DEFAULT falhar, executa failover imediato para o FALLBACK.
 * 4. Registro atômico do pagamento liquidado no Redis para auditoria do Banco Central.
 */
@Component
public class QueueWorker {

    private static final Logger log = LoggerFactory.getLogger(QueueWorker.class);

    private final RedisStorage storage;
    private final GatewayClient client;
    private final CampeaoConfig config;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService workerPool;

    public QueueWorker(RedisStorage storage, GatewayClient client, CampeaoConfig config) {
        this.storage = storage;
        this.client = client;
        this.config = config;
    }

    @PostConstruct
    public void startWorkers() {
        running.set(true);
        int concurrency = config.getWorkerConcurrency();
        this.workerPool = Executors.newVirtualThreadPerTaskExecutor();

        log.info("🚀 [QUEUE WORKER] Iniciando {} workers assíncronos em Virtual Threads...", concurrency);

        for (int i = 0; i < concurrency; i++) {
            final int workerId = i + 1;
            workerPool.submit(() -> runWorkerLoop(workerId));
        }
    }

    private void runWorkerLoop(int workerId) {
        while (running.get()) {
            try {
                QueuedPayment payment = storage.popPayment();

                if (payment == null) {
                    // Fila vazia: sleep de 20ms para aliviar completamente o Redis e economizar CPU
                    Thread.sleep(20);
                    continue;
                }

                processSinglePayment(payment);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[Worker {}] Erro inesperado ao processar pagamento: {}", workerId, e.getMessage());
            }
        }
    }

    private void processSinglePayment(QueuedPayment payment) {
        boolean defaultHealthy = storage.isGatewayHealthy("default");

        // 1. Tentar DEFAULT se estiver saudável
        if (defaultHealthy) {
            ProcessorResponse response = client.sendPayment(config.getDefaultGatewayUrl(), payment);

            if (response.success()) {
                storage.recordSettledPayment("default", payment);
                return;
            }

            // O Default falhou: notifica no storage e escala para o fallback em voo
            log.warn("⚡ [QUEUE WORKER] Default falhou para {}. Escalando para FALLBACK...", payment.correlationId());
            storage.setGatewayHealthy("default", false);
        }

        // 2. Envio para FALLBACK (Contingência)
        ProcessorResponse fallbackResponse = client.sendPayment(config.getFallbackGatewayUrl(), payment);

        if (fallbackResponse.success()) {
            storage.recordSettledPayment("fallback", payment);
        } else {
            log.error("❌ [QUEUE WORKER] Ambos processadores falharam para {}", payment.correlationId());
        }
    }

    @PreDestroy
    public void stopWorkers() {
        running.set(false);
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }
}

