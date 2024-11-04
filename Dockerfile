# Use an official Ubuntu base image
FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install required dependencies
RUN apt-get update && \
    apt-get install -y wget sudo software-properties-common && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y maven && \
    apt-get install -y make cmake gcc g++ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for JDK 1.8
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy the MorphStream project directory from the host to the container
COPY . /workspace/MorphStream

# Set the working directory to MorphStream
WORKDIR /workspace/MorphStream

# Display JDK and Maven versions to confirm installation
RUN java -version && mvn -version

# Default command
CMD ["bash"]
