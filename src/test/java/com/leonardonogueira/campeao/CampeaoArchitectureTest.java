package com.leonardonogueira.campeao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.campeao.api.PaymentController;
import com.leonardonogueira.campeao.config.CampeaoConfig;
import com.leonardonogueira.campeao.model.CampeaoModels.*;
import com.leonardonogueira.campeao.storage.RedisStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CampeaoArchitectureTest {

    private PaymentController controller;
    private RedisStorage storage;

    @BeforeEach
    void setUp() {
        CampeaoConfig config = new CampeaoConfig();
        ObjectMapper objectMapper = new ObjectMapper();
        this.storage = new RedisStorage(config, objectMapper);
        this.controller = new PaymentController(storage);
        this.storage.purge();
    }

    @Test
    @DisplayName("Deve enfileirar pagamento com sucesso e responder em sub-milissegundo")
    void shouldEnqueuePaymentSuccessfully() {
        UUID correlationId = UUID.randomUUID();
        PaymentRequest request = new PaymentRequest(correlationId, new BigDecimal("100.00"), Instant.now().toString());

        ResponseEntity<Map<String, Object>> response = controller.processPayment(request);

        assertEquals(200, response.getStatusCode().value());
        assertEquals("QUEUED", response.getBody().get("status"));
        assertEquals(correlationId, response.getBody().get("correlationId"));
    }

    @Test
    @DisplayName("Deve registrar pagamento liquidado no storage e responder o summary auditável")
    void shouldRecordAndQuerySummaryReport() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        // Simula a liquidação realizada pelo QueueWorker
        storage.recordSettledPayment("default", new QueuedPayment(id1, new BigDecimal("150.00"), Instant.now()));
        storage.recordSettledPayment("fallback", new QueuedPayment(id2, new BigDecimal("250.00"), Instant.now()));

        ResponseEntity<SummaryReport> response = controller.getSummary(null, null);

        assertEquals(200, response.getStatusCode().value());
        SummaryReport report = response.getBody();
        assertNotNull(report);

        assertEquals(1, report.defaultSummary().totalRequests());
        assertEquals(new BigDecimal("150.00"), report.defaultSummary().totalAmount());

        assertEquals(1, report.fallbackSummary().totalRequests());
        assertEquals(new BigDecimal("250.00"), report.fallbackSummary().totalAmount());
    }

    @Test
    @DisplayName("Deve limpar o estado do storage ao chamar purge")
    void shouldPurgeStorageSuccessfully() {
        storage.recordSettledPayment("default", new QueuedPayment(UUID.randomUUID(), new BigDecimal("50.00"), Instant.now()));

        controller.purge();

        SummaryReport report = controller.getSummary(null, null).getBody();
        assertNotNull(report);
        assertEquals(0, report.defaultSummary().totalRequests());
        assertEquals(BigDecimal.ZERO, report.defaultSummary().totalAmount());
    }
}

