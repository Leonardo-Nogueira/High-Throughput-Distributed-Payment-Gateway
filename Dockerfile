# Runtime Stage com tuning de alta performance e baixo overhead de memória
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/high-throughput-payment-gateway-0.0.1-SNAPSHOT.jar app.jar

# Flags de baixa latência e contenção mínima de memória nativa:
# -XX:+UseSerialGC: Consome apenas ~2MB de overhead nativo contra ~40MB do G1GC
# -XX:TieredStopAtLevel=1: Reduz uso de JIT compiler e CodeCache
# -Xms128m -Xmx256m: Heap ajustado para picos de alta vazão
ENV JAVA_OPTS="-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xms128m -Xmx256m -XX:MaxMetaspaceSize=64m -Xss256k"

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
