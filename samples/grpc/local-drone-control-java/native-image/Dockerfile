FROM debian:11-slim as builder

ARG GRAAL_VERSION="17.0.8"
ARG MAVEN_VERSION="3.9.4"

SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y build-essential curl unzip zip zlib1g-dev
RUN curl -s "https://get.sdkman.io" | bash
RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && \
    sdk install java ${GRAAL_VERSION}-graalce && \
    sdk install maven ${MAVEN_VERSION}

WORKDIR /opt/build
COPY . .
RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && \
    mvn -DskipTests=true -Pnative compile package

FROM gcr.io/distroless/java-base-debian11:nonroot
COPY --from=builder /opt/build/target/local-drone-control /bin/
EXPOSE 8080
ENTRYPOINT ["/bin/local-drone-control"]
CMD []
