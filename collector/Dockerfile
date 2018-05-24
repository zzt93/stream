FROM 192.168.1.202/common/basejava
RUN mkdir /data
VOLUME /data
ADD ./target/boot/collector-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 5000
ENTRYPOINT ["java","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5000", "-jar", "/app.jar"]
