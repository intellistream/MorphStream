FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
    openjdk-8-jdk \
    curl \
    rdma-core \
    librdmacm1 \
    librdmacm-dev \
    libibverbs-dev && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /rtfaas
ENV LIBDIR="/rtfaas/lib"
ENV JAR="/rtfaas/jar/rtfaas-jar-with-dependencies.jar"
ENV RSTDIR="/rtfaas/results"

RUN mkdir -p $LIBDIR
RUN mkdir -p $RSTDIR
RUN mkdir -p /rtfaas/jar

ADD morph-clients/target/rtfaas-jar-with-dependencies.jar /rtfaas/jar/rtfaas-jar-with-dependencies.jar
ADD morph-lib/lib/* /rtfaas/lib/
ADD scripts/FaaS/DockerFiles/entrypoint.sh entrypoint.sh
ADD scripts/FaaS/DockerFiles/driver.sh driver.sh
ADD scripts/FaaS/DockerFiles/worker.sh worker.sh
ADD scripts/FaaS/DockerFiles/client.sh client.sh
ADD scripts/FaaS/DockerFiles/database.sh database.sh

RUN chmod +x /rtfaas/entrypoint.sh
RUN chmod +x /rtfaas/driver.sh
RUN chmod +x /rtfaas/worker.sh
RUN chmod +x /rtfaas/client.sh
RUN chmod +x /rtfaas/database.sh

ENTRYPOINT ["bash", "/rtfaas/entrypoint.sh"]
#CMD ["tail", "-f", "/dev/null"]
#sudo bash worker.sh 2 SocialNetwork 20 20 20 4 20 D 3