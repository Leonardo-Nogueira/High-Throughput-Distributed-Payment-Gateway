package com.leonardonogueira.campeao.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Equivalente 1:1 ao 'models.ts' do projeto vencedor da Rinha 2025.
 * Define os modelos imutáveis de transação, resumo do Banco Central e status dos gateways.
 */
public final class CampeaoModels {

    private CampeaoModels() {}

    /**
     * Payload recebido no POST /payments
     */
    public record PaymentRequest(
        UUID correlationId,
        BigDecimal amount,
        String requestedAt
    ) {
        public PaymentRequest {
            if (requestedAt == null || requestedAt.isBlank()) {
                requestedAt = Instant.now().toString();
            }
        }
    }

    /**
     * Tarefa colocada na fila pelo POST /payments para processamento assíncrono
     */
    public record QueuedPayment(
        UUID correlationId,
        BigDecimal amount,
        Instant requestedAt
    ) {}

    /**
     * Resposta da chamada HTTP para os Payment Processors (Default ou Fallback)
     */
    public record ProcessorResponse(
        boolean success,
        int statusCode,
        String processorName,
        long elapsedMicros,
        String error
    ) {}

    /**
     * Status retornado pelo endpoint /payments/service-health do processador
     */
    public record GatewayHealth(
        boolean failing,
        int minResponseTime,
        int statusCode
    ) {}

    /**
     * Resumo oficial auditado pelo Banco Central via GET /payments-summary
     */
    public record SummaryReport(
        @JsonProperty("default") ProcessorSummary defaultSummary,
        @JsonProperty("fallback") ProcessorSummary fallbackSummary
    ) {}

    public record ProcessorSummary(
        long totalRequests,
        BigDecimal totalAmount
    ) {
        public static ProcessorSummary empty() {
            return new ProcessorSummary(0, BigDecimal.ZERO);
        }
    }
}

