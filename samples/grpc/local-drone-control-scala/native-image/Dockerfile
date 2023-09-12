FROM sbtscala/scala-sbt:eclipse-temurin-focal-17.0.5_8_1.9.4_2.13.12 as builder
RUN apt-get update && apt-get install -y build-essential zlib1g-dev
USER sbtuser
WORKDIR /home/sbtuser/build
COPY --chown=sbtuser:sbtuser . .
RUN sbt nativeImage

FROM gcr.io/distroless/java-base-debian11:nonroot
COPY --from=builder /home/sbtuser/build/target/native-image/local-drone-control /bin/
EXPOSE 8080
ENTRYPOINT ["/bin/local-drone-control"]
CMD []
