# How to run
Use the following Gradle command to run the benchmarks:
```
liiklus $ ./gradlew --console=plain --no-daemon benchmark:jmh
```

# Warning
Kafka storage benchmark will download a small (~160Mb) dataset from Twitter.
