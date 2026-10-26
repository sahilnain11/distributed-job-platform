# Verified multi-platform Java 21 JRE; update digest deliberately, not implicitly.
FROM eclipse-temurin:21-jre-jammy@sha256:affcab5356e1270ac35d2361ee83135af2f03e8d580eca78a6d5d6fdbf048db0
WORKDIR /app
COPY --chown=10001:10001 target/distributed-job-platform-0.0.1-SNAPSHOT.jar /app/platform.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=65", "-jar", "/app/platform.jar"]