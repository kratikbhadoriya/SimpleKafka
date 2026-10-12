# Build the shaded jar, then run it on a JRE-only image.
FROM maven:3-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/target/SimpleKafka-1.0-SNAPSHOT.jar /app/simplekafka.jar
# Brokers write their logs to ./data relative to the working directory.
VOLUME /app/data
