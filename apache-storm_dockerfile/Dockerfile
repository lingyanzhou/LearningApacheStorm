FROM debian
WORKDIR /opt
COPY sources.list /etc/apt/sources.list
RUN apt-get -y update && \
    apt-get -y install openjdk-8-jdk \
                       python \
                       python3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY apache-storm-1.2.2.tar.gz apache-storm-1.2.2.tar.gz
RUN tar -zxf apache-storm-1.2.2.tar.gz && \
    mkdir -p /var/storm/data && \
    mkdir -p /var/storm/app && \
    mkdir -p /var/storm/app/lib
COPY storm.yaml /opt/apache-storm-1.2.2/conf/storm.yaml
ENV PATH="/opt/apache-storm-1.2.2/bin:${PATH}"

EXPOSE 6627 8080
CMD storm



