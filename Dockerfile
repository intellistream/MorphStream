FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk curl && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /rtfaas
ENV LIBDIR="/rtfaas/lib"
ENV JAR="/rtfaas/jar/rtfaas-jar-with-dependencies.jar"
ENV RSTDIR="/rtfaas/results"

RUN mkdir -p $LIBDIR
RUN mkdir -p $RSTDIR
RUN mkdir -p /rtfaas/jar

ADD morph-clients/target/rtfaas-jar-with-dependencies.jar /rtfaas/jar/rtfaas-jar-with-dependencies.jar
ADD morph-lib/lib/* /rtfaas/lib/
ADD scripts/FaaS/driver.sh driver.sh

ENTRYPOINT ["bash", "driver.sh"]

