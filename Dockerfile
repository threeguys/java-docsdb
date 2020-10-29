FROM maven:3.6.3-adoptopenjdk-14 as builder

RUN mkdir -p /opt/build/src /opt/build/output
COPY . /opt/build/src
RUN cd /opt/build/src && mvn package -Doutput.dir=/opt/build/output

FROM adoptopenjdk:14-jre-hotspot
RUN mkdir -p /opt/app/lib

COPY --from=builder /opt/build/output/lib /opt/app/lib
COPY --from=builder /opt/build/output/java-docsdb-server-0.0.1-SNAPSHOT.jar /opt/app

WORKDIR /opt/app
EXPOSE 8080

CMD [ "java", "-jar", "java-docsdb-server-0.0.1-SNAPSHOT.jar" ]
