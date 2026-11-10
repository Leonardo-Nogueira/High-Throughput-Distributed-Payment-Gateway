package com.leonardonogueira.campeao.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.campeao.config.CampeaoConfig;
import com.leonardonogueira.campeao.model.CampeaoModels.*;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Equivalente 1:1 ao 'client.ts' do projeto vencedor da Rinha 2025.
 * 
 * Implementa o cliente HTTP com conexão persistente TCP Keep-Alive e Virtual Threads.
 * É usado pelos workers assíncronos para disparar transações e verificar a saúde dos processadores.
 */
@Component
public class GatewayClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration requestTimeout;

    public GatewayClient(CampeaoConfig config, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.requestTimeout = Duration.ofMillis(config.getGatewayTimeoutMs());

        HttpClient client = null;
        try {
            client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(500))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        } catch (Exception ignored) {}
        this.httpClient = client;
    }

    public ProcessorResponse sendPayment(String baseUrl, QueuedPayment payment) {
        if (httpClient == null) {
            return new ProcessorResponse(false, 503, baseUrl, 0, "HttpClient not initialized");
        }
        long start = System.nanoTime();
        try {
            String json = objectMapper.writeValueAsString(Map.of(
                "correlationId", payment.correlationId().toString(),
                "amount", payment.amount(),
                "requestedAt", payment.requestedAt().toString()
            ));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/payments"))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long elapsedMicros = (System.nanoTime() - start) / 1_000;

            boolean success = response.statusCode() >= 200 && response.statusCode() < 300;
            return new ProcessorResponse(success, response.statusCode(), baseUrl, elapsedMicros, null);

        } catch (Exception e) {
            long elapsedMicros = (System.nanoTime() - start) / 1_000;
            return new ProcessorResponse(false, 503, baseUrl, elapsedMicros, e.getMessage());
        }
    }

    public GatewayHealth checkHealth(String baseUrl) {
        if (httpClient == null) {
            return new GatewayHealth(true, 9999, 503);
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/payments/service-health"))
                .timeout(Duration.ofSeconds(2))
                .header("Accept", "application/json")
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                var jsonNode = objectMapper.readTree(response.body());
                boolean failing = jsonNode.path("failing").asBoolean(false);
                int minResponseTime = jsonNode.path("minResponseTime").asInt(0);
                return new GatewayHealth(failing, minResponseTime, 200);
            }

            return new GatewayHealth(true, 9999, response.statusCode());
        } catch (Exception e) {
            return new GatewayHealth(true, 9999, 500);
        }
    }
}

