FROM 192.168.1.202/common/basejava
RUN mkdir /data
VOLUME /data
ADD ./target/boot/streamer-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 5001
ENTRYPOINT ["java","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5001", "-jar", "/app.jar"]
