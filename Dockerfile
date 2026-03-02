FROM gradle:8.7-jdk25 as builder
USER root
COPY . .
RUN gradle --no-daemon build -x test

FROM gcr.io/distroless/java25
ENV JAVA_TOOL_OPTIONS -XX:+ExitOnOutOfMemoryError
COPY --from=builder /home/gradle/build/libs/fint-core-consumer*.jar /data/app.jar
CMD ["/data/app.jar"]
