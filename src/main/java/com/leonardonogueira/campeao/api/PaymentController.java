package com.leonardonogueira.campeao.api;

import com.leonardonogueira.campeao.model.CampeaoModels.*;
import com.leonardonogueira.campeao.storage.RedisStorage;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * Equivalente 1:1 ao 'main.ts' do projeto vencedor da Rinha 2025.
 * 
 * Implementa os endpoints HTTP no estilo Fastify:
 * 1. POST /payments: Enfileira a transação e responde imediatamente com 200 OK.
 * 2. GET /payments-summary: Consulta o sumário agregado no Redis em sub-milissegundo.
 * 3. POST /purge-payments: Limpa as filas e chaves do Redis antes do teste de carga do k6.
 */
@RestController
public class PaymentController {

    private final RedisStorage storage;

    public PaymentController(RedisStorage storage) {
        this.storage = storage;
    }

    /**
     * ENDPOINT OFICIAL 1: POST /payments (versão Campeã com Fila Assíncrona)
     */
    @PostMapping("/payments")
    public ResponseEntity<Map<String, Object>> processPayment(@RequestBody PaymentRequest request) {
        Instant requestedAt = (request.requestedAt() != null)
            ? Instant.parse(request.requestedAt())
            : Instant.now();

        QueuedPayment queuedPayment = new QueuedPayment(
            request.correlationId(),
            request.amount(),
            requestedAt
        );

        // Desacoplamento total: Enfileira no Redis e responde imediatamente em sub-milissegundo
        storage.pushPayment(queuedPayment);

        return ResponseEntity.ok(Map.of(
            "status", "QUEUED",
            "correlationId", request.correlationId()
        ));
    }

    /**
     * ENDPOINT OFICIAL 2: GET /payments-summary (Auditoria do Banco Central)
     */
    @GetMapping("/payments-summary")
    public ResponseEntity<SummaryReport> getSummary(
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to
    ) {
        SummaryReport report = storage.getSummary(from, to);
        return ResponseEntity.ok(report);
    }

    /**
     * Endpoint chamado pelo k6 para limpar o estado antes do teste
     */
    @PostMapping("/purge-payments")
    public ResponseEntity<Map<String, String>> purge() {
        storage.purge();
        return ResponseEntity.ok(Map.of("message", "Campeao storage and queues purged successfully"));
    }
}

