FROM openjdk:8-jdk-alpine3.8
LABEL authors="curryzjj"
ADD morph-clients/target/rtfaas-jar-with-dependencies.jar rtfaas-jar-with-dependencies.jar
ENTRYPOINT ["java", "-jar", "rtfaas-jar-with-dependencies.jar"]

