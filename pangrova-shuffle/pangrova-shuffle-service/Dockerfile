FROM eclipse-temurin:17-jre

RUN useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin pangrova
WORKDIR /opt/pangrova
COPY target/pangrova-shuffle-service-0.1.0-SNAPSHOT.jar shuffle-service.jar
USER 10001
EXPOSE 7443
ENTRYPOINT ["java", "-jar", "/opt/pangrova/shuffle-service.jar", "/etc/pangrova/shuffle-service.yml"]
