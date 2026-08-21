# =============================================================================
# Java Coordinator - Dockerfile
# Multi-stage build: sbt + JDK to slim JRE runtime
# =============================================================================

# ---- Stage 1: Build with sbt + Azul Zulu JDK 21 ----
FROM azul/zulu-openjdk-alpine:21 AS builder

# Install sbt
ARG SBT_VERSION=1.12.14
RUN apk add --no-cache bash curl python3 tar \
    && curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      -o /tmp/sbt.tgz \
    && echo "cd17daae220ff264faa4251334522444518584f0eb2ee82da01523a9b9002b7e  /tmp/sbt.tgz" \
      | sha256sum -c - \
    && tar xzf /tmp/sbt.tgz -C /opt \
    && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt \
    && rm /tmp/sbt.tgz

WORKDIR /app
COPY scripts/octopus_image_contract.py /usr/local/bin/octopus-image-contract
COPY services/octopus/ ./
RUN sbt --batch assembly \
    && python3 /usr/local/bin/octopus-image-contract jar target/scala-3.*/octopus.jar

# ---- Stage 2: Runtime with Azul Zulu JRE 21 (Alpine) ----
FROM azul/zulu-openjdk-alpine:21-jre
ARG PARENT_COMMIT
ARG OCTOPUS_COMMIT
ENV TZ=America/New_York
ENV JAVA_TOOL_OPTIONS="-XX:+UseZGC -XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=75"

RUN test -n "$PARENT_COMMIT" \
    && test -n "$OCTOPUS_COMMIT"

LABEL org.opencontainers.image.revision="$PARENT_COMMIT" \
      io.ssl-proxy.octopus.revision="$OCTOPUS_COMMIT"

RUN apk add --no-cache \
        bash \
        ca-certificates \
        tzdata

WORKDIR /app

RUN addgroup -g 1000 coordinator \
    && adduser -u 1000 -G coordinator -S coordinator

# Copy the assembled fat JAR
COPY --chown=coordinator:coordinator --from=builder /app/target/scala-3.*/octopus.jar /app/octopus.jar

COPY --chown=coordinator:coordinator docker/redpanda /app/docker/redpanda

RUN chown coordinator:coordinator /app \
    && chmod 500 /app \
    && chmod 400 /app/octopus.jar \
    && chmod -R a=rX /app/docker/redpanda

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=20s \
  CMD wget -qO- http://localhost:8081/health || exit 1

USER 1000:1000

ENTRYPOINT ["java", "-jar", "/app/octopus.jar"]
