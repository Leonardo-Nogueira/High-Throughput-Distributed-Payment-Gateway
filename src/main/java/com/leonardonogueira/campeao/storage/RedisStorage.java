package com.leonardonogueira.campeao.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.campeao.config.CampeaoConfig;
import com.leonardonogueira.campeao.model.CampeaoModels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.resps.Tuple;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Equivalente 1:1 ao 'storage.ts' do projeto vencedor da Rinha 2025.
 * 
 * O Ricássio usou o Redis como:
 * 1. Fila compartilhada entre as instâncias da API (LPUSH / RPOP).
 * 2. Estado compartilhado do Circuit Breaker (SET rinha:health:default).
 * 3. Livro contábil ultrarrápido (Sorted Sets com timestamp em ZADD e Hashes com HINCRBY).
 * 
 * Implementa conexão direta e ultraleve com Jedis (pool C-like, sem overhead de Netty).
 * Se o Redis não estiver rodando (ex: testes locais rápidos sem Docker), ela cai graciosamente
 * para estruturas em memória com aviso no log.
 */
@Service
public class RedisStorage {

    private static final Logger log = LoggerFactory.getLogger(RedisStorage.class);

    private static final String QUEUE_KEY = "rinha:payments:queue";
    private static final String DEFAULT_HEALTH_KEY = "rinha:health:default";
    private static final String FALLBACK_HEALTH_KEY = "rinha:health:fallback";
    private static final String SUMMARY_DEFAULT_ZSET = "rinha:summary:default:zset";
    private static final String SUMMARY_FALLBACK_ZSET = "rinha:summary:fallback:zset";

    private final CampeaoConfig config;
    private final ObjectMapper objectMapper;
    private JedisPool jedisPool;
    private final AtomicBoolean redisAvailable = new AtomicBoolean(false);

    // Fallback em memória para quando o Redis não estiver ativo localmente
    private final ConcurrentLinkedQueue<QueuedPayment> memoryQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, Boolean> memoryHealth = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, QueuedPayment> memoryDefaultPayments = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, QueuedPayment> memoryFallbackPayments = new ConcurrentHashMap<>();

    // Cache local volátil para evitar round-trips contínuos no Redis a cada pagamento processado
    private final AtomicBoolean defaultHealthyCached = new AtomicBoolean(true);
    private final AtomicBoolean fallbackHealthyCached = new AtomicBoolean(true);
    private volatile long lastHealthReadTime = 0;

    public RedisStorage(CampeaoConfig config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
        initJedis();
    }

    private void initJedis() {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(256);
            poolConfig.setMaxIdle(128);
            poolConfig.setMinIdle(32);
            poolConfig.setMaxWait(Duration.ofMillis(500));

            this.jedisPool = new JedisPool(poolConfig, config.getRedisHost(), config.getRedisPort(), 200);
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
                redisAvailable.set(true);
                log.info("✅ [REDIS STORAGE] Conexão estabelecida com Redis em {}:{}", config.getRedisHost(), config.getRedisPort());
            }
        } catch (Exception e) {
            redisAvailable.set(false);
            log.warn("⚠️ [REDIS STORAGE] Redis não encontrado em {}:{}. Operando em modo In-Memory Fallback.",
                config.getRedisHost(), config.getRedisPort());
        }
    }

    /**
     * Enfileira o pagamento (LPUSH no Redis ou queue em memória)
     */
    public void pushPayment(QueuedPayment payment) {
        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                String json = objectMapper.writeValueAsString(payment);
                jedis.lpush(QUEUE_KEY, json);
                return;
            } catch (Exception e) {
                log.warn("Erro ao enviar para fila no Redis, fallback para memória: {}", e.getMessage());
            }
        }
        memoryQueue.offer(payment);
    }

    /**
     * Consome um pagamento da fila (RPOP no Redis ou poll da memória)
     */
    public QueuedPayment popPayment() {
        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                String json = jedis.rpop(QUEUE_KEY);
                if (json != null) {
                    return objectMapper.readValue(json, QueuedPayment.class);
                }
                return null;
            } catch (Exception ignored) {}
        }
        return memoryQueue.poll();
    }

    /**
     * Atualiza o estado de saúde do processador no Redis e no cache local
     */
    public void setGatewayHealthy(String gatewayName, boolean healthy) {
        if ("default".equalsIgnoreCase(gatewayName)) {
            defaultHealthyCached.set(healthy);
        } else {
            fallbackHealthyCached.set(healthy);
        }
        lastHealthReadTime = System.currentTimeMillis();

        String key = "default".equalsIgnoreCase(gatewayName) ? DEFAULT_HEALTH_KEY : FALLBACK_HEALTH_KEY;
        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set(key, String.valueOf(healthy));
                return;
            } catch (Exception ignored) {}
        }
        memoryHealth.put(key, healthy);
    }

    /**
     * Lê o estado de saúde do processador (com cache de 250ms para reduzir 99% das chamadas ao Redis)
     */
    public boolean isGatewayHealthy(String gatewayName) {
        long now = System.currentTimeMillis();
        if (now - lastHealthReadTime > 250) {
            if (redisAvailable.get()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    String defVal = jedis.get(DEFAULT_HEALTH_KEY);
                    if (defVal != null) {
                        defaultHealthyCached.set(Boolean.parseBoolean(defVal));
                    }
                    String fallVal = jedis.get(FALLBACK_HEALTH_KEY);
                    if (fallVal != null) {
                        fallbackHealthyCached.set(Boolean.parseBoolean(fallVal));
                    }
                    lastHealthReadTime = now;
                } catch (Exception ignored) {}
            }
        }
        return "default".equalsIgnoreCase(gatewayName) ? defaultHealthyCached.get() : fallbackHealthyCached.get();
    }

    /**
     * Registra pagamento liquidado no ZSET (Sorted Set) do Redis indexado por epoch milli
     */
    public void recordSettledPayment(String processorName, QueuedPayment payment) {
        long epochMilli = payment.requestedAt().toEpochMilli();
        String member = payment.correlationId() + ":" + payment.amount();
        String zsetKey = "default".equalsIgnoreCase(processorName) ? SUMMARY_DEFAULT_ZSET : SUMMARY_FALLBACK_ZSET;

        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.zadd(zsetKey, (double) epochMilli, member);
                return;
            } catch (Exception ignored) {}
        }

        if ("default".equalsIgnoreCase(processorName)) {
            memoryDefaultPayments.put(epochMilli, payment);
        } else {
            memoryFallbackPayments.put(epochMilli, payment);
        }
    }

    /**
     * Consulta o sumário exigido pelo Banco Central (com suporte opcional a filtro temporal from/to)
     */
    public SummaryReport getSummary(Instant from, Instant to) {
        long fromMilli = (from != null) ? from.toEpochMilli() : 0;
        long toMilli = (to != null) ? to.toEpochMilli() : Long.MAX_VALUE;

        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                ProcessorSummary defSummary = queryZSet(jedis, SUMMARY_DEFAULT_ZSET, fromMilli, toMilli);
                ProcessorSummary fallSummary = queryZSet(jedis, SUMMARY_FALLBACK_ZSET, fromMilli, toMilli);
                return new SummaryReport(defSummary, fallSummary);
            } catch (Exception e) {
                log.error("Erro ao consultar summary no Redis: {}", e.getMessage());
            }
        }

        // Fallback em memória
        ProcessorSummary defSummary = aggregateMemory(memoryDefaultPayments, fromMilli, toMilli);
        ProcessorSummary fallSummary = aggregateMemory(memoryFallbackPayments, fromMilli, toMilli);
        return new SummaryReport(defSummary, fallSummary);
    }

    private ProcessorSummary queryZSet(Jedis jedis, String key, long fromMilli, long toMilli) {
        List<Tuple> items = jedis.zrangeByScoreWithScores(key, (double) fromMilli, (double) toMilli);
        long count = 0;
        BigDecimal total = BigDecimal.ZERO;

        for (Tuple t : items) {
            String member = t.getElement();
            int colonIdx = member.indexOf(':');
            if (colonIdx > 0) {
                String amtStr = member.substring(colonIdx + 1);
                total = total.add(new BigDecimal(amtStr));
                count++;
            }
        }
        return new ProcessorSummary(count, total);
    }

    private ProcessorSummary aggregateMemory(Map<Long, QueuedPayment> map, long fromMilli, long toMilli) {
        long count = 0;
        BigDecimal total = BigDecimal.ZERO;
        for (Map.Entry<Long, QueuedPayment> entry : map.entrySet()) {
            if (entry.getKey() >= fromMilli && entry.getKey() <= toMilli) {
                count++;
                total = total.add(entry.getValue().amount());
            }
        }
        return new ProcessorSummary(count, total);
    }

    public void purge() {
        if (redisAvailable.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(QUEUE_KEY, SUMMARY_DEFAULT_ZSET, SUMMARY_FALLBACK_ZSET);
            } catch (Exception ignored) {}
        }
        memoryQueue.clear();
        memoryDefaultPayments.clear();
        memoryFallbackPayments.clear();
    }
}

