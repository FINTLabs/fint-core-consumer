FROM gradle:9.3-jdk25 as builder
USER root
COPY . .
RUN gradle --no-daemon bootJar

FROM eclipse-temurin:25-jdk-alpine
ENV JAVA_TOOL_OPTIONS="-XX:+ExitOnOutOfMemoryError"
COPY --from=builder /home/gradle/build/libs/fint-core-consumer*.jar /data/app.jar
ENTRYPOINT ["java", "-jar", "/data/app.jar"]
