FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /workspace

# Copy dependency metadata first to leverage Docker layer cache
COPY pom.xml .
RUN mvn -B -e -q dependency:go-offline

# Copy project sources and build the runnable jar
COPY src ./src
RUN mvn -B -DskipTests clean package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Copy the shaded jar produced in the builder stage
COPY --from=builder /workspace/target/*.jar /app/app.jar

EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
