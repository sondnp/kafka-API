#Start with a base image containing Java runtime
FROM openjdk:17-jdk-slim as build

#Information around who maintains the image
MAINTAINER sondn.com

#RUN mvn install
#RUN mvn clean package -Dmaven.test.skip=true

# Add the application's jar to the container
COPY target/kafka-sdk-0.0.1-SNAPSHOT.jar kafka_sdk-0.0.1-SNAPSHOT.jar

#execute the application
ENTRYPOINT ["java","-jar","/kafka_sdk-0.0.1-SNAPSHOT.jar"]