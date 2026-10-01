# syntax=docker/dockerfile:1.4
#
# WPaxos benchmark image. Stage 1 compiles core+sample with JDK 8 and unpacks the runtime
# dependency set; stage 2 ships those jars plus a JRE and nothing else.
#
# This file is deliberately self-contained on public registry images. The buildx
# `docker-container` driver cannot see images in the daemon's local store, so any
# `FROM <locally-built-tag>` fails with "pull access denied". `FROM ... AS build` below is a
# stage reference inside this one file, not a registry reference, and is safe.
#
# Architecture note: the build succeeds on linux/arm64, but two native libraries do not.
#   - org.fusesource.leveldbjni:leveldbjni-all:1.8 (2013) ships only
#     META-INF/native/{linux32,linux64,osx,windows32,windows64}. There is no aarch64 native,
#     so indexType=0 (LevelDB) cannot run on linux/arm64 at all.
#   - org.rocksdb:rocksdbjni:5.15.10 as resolved here ships only linux32.so, linux64.so,
#     linux-ppc64le.so and osx.jnilib, so there is no aarch64 native either.
# Neither is substituted or stubbed: the arms of the benchmark that need them are simply not
# runnable on arm64, and the arm64 image exists for the file-index (indexType=1) scenes.
#
# Provenance is stamped from build ARGs and written to /opt/wpaxos/PROVENANCE, so
# `docker inspect` plus `docker run --entrypoint cat ... PROVENANCE` on a pulled image
# identifies exactly which commit produced it and which base images it was built from.

ARG BUILD_CREATED=unknown
ARG VCS_REF=unknown
ARG SOURCE_URL=https://github.com/simbo1905/WPaxos

FROM maven:3.9-eclipse-temurin-8 AS build
WORKDIR /src
COPY pom.xml /src/pom.xml
COPY core/pom.xml /src/core/pom.xml
COPY sample/pom.xml /src/sample/pom.xml
# Warm the dependency cache against the poms alone so source-only edits do not re-resolve.
# The protobuf plugin fetches a protoc binary during compile, so go-offline alone is expected
# to be incomplete; the real build below is the one that must succeed.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -q -DskipTests dependency:go-offline || true
COPY core /src/core
COPY sample /src/sample
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -DskipTests package

FROM eclipse-temurin:8-jre
ARG BUILD_CREATED
ARG VCS_REF
ARG SOURCE_URL
ARG BASE_BUILD_IMAGE=maven:3.9-eclipse-temurin-8
ARG BASE_RUNTIME_IMAGE=eclipse-temurin:8-jre
ARG BASE_BUILD_DIGEST=unresolved
ARG BASE_RUNTIME_DIGEST=unresolved

LABEL org.opencontainers.image.title="WPaxos" \
      org.opencontainers.image.description="WPaxos consensus library and the simple-sample 3-node load generator" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_CREATED}" \
      io.github.simbo1905.base-build-image="${BASE_BUILD_IMAGE}@${BASE_BUILD_DIGEST}" \
      io.github.simbo1905.base-runtime-image="${BASE_RUNTIME_IMAGE}@${BASE_RUNTIME_DIGEST}"

WORKDIR /opt/wpaxos
COPY --from=build /src/core/target/wpaxos.core-1.0.1.jar /opt/wpaxos/lib/
COPY --from=build /src/core/target/dependency/ /opt/wpaxos/lib/
COPY --from=build /src/sample/target/wpaxos.sample-1.0.1.jar /opt/wpaxos/lib/
COPY --from=build /src/sample/target/dependency/ /opt/wpaxos/lib/
COPY docker/run.sh /opt/wpaxos/run.sh
COPY docker/conf /opt/wpaxos/conf

RUN chmod +x /opt/wpaxos/run.sh \
 && mkdir -p /opt/wpaxos/data \
 && { printf 'git.revision=%s\n' "${VCS_REF}"; \
      printf 'build.created=%s\n' "${BUILD_CREATED}"; \
      printf 'build.source=%s\n' "${SOURCE_URL}"; \
      printf 'base.build=%s@%s\n' "${BASE_BUILD_IMAGE}" "${BASE_BUILD_DIGEST}"; \
      printf 'base.runtime=%s@%s\n' "${BASE_RUNTIME_IMAGE}" "${BASE_RUNTIME_DIGEST}"; \
    } > /opt/wpaxos/PROVENANCE \
 && sha256sum /opt/wpaxos/lib/*.jar >> /opt/wpaxos/PROVENANCE \
 && java -version

ENTRYPOINT ["/opt/wpaxos/run.sh"]
