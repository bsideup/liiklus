FROM openjdk:11.0.2-jdk AS workspace

WORKDIR /root/project
COPY gradle gradle/
COPY gradlew ./
RUN ./gradlew --no-daemon --version

ENV TERM=dumb

COPY  build.gradle ./
RUN ./gradlew --info --no-daemon --console=plain downloadDependencies

COPY . .

RUN ./gradlew --no-daemon --info --console=plain build -x check

FROM openjdk:11-jre

WORKDIR /app

RUN java -Xshare:dump

COPY --from=workspace /root/project/app/build/libs/app-boot.jar app.jar
COPY --from=workspace /root/project/plugins/*/build/libs/*.jar plugins/

ENV JAVA_OPTS=""
ENV JAVA_MEMORY_OPTS="-XX:+ExitOnOutOfMemoryError -XshowSettings:vm -noverify"

CMD ["sh", "-c", "java -Xshare:on $JAVA_MEMORY_OPTS $JAVA_OPTS -jar app.jar"]
