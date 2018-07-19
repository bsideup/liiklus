FROM openjdk:8-jdk AS workspace

COPY . /root/project

WORKDIR /root/project

RUN ./gradlew --no-daemon build -x check

FROM openjdk:8-jre

WORKDIR /app

COPY --from=workspace /root/project/app/build/libs/app.jar app.jar
COPY --from=workspace /root/project/plugins/*/build/libs/*.jar plugins/

ENV JAVA_OPTS=""
ENV JAVA_MEMORY_OPTS="-XX:+ExitOnOutOfMemoryError -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:+UseG1GC -XshowSettings:vm"

CMD ["sh", "-c", "java $JAVA_MEMORY_OPTS $JAVA_OPTS -jar app.jar"]
