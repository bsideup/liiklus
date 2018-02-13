FROM openjdk:8u151-jdk AS workspace

WORKDIR /project

COPY . .

RUN gradle --no-daemon build -x test

FROM openjdk:8u151-jre

COPY --from=workspace /project/app/build/libs/app.jar /app.jar

ENV JAVA_OPTS=""
ENV JAVA_MEMORY_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:+UseG1GC"

CMD ["sh", "-c", "java $JAVA_OPTS $JAVA_MEMORY_OPTS -jar /app.jar"]
