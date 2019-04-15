FROM debian
WORKDIR /opt
COPY sources.list /etc/apt/sources.list
RUN apt-get -y update && \
    apt-get -y install openjdk-8-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY zookeeper-3.4.14.tar.gz zookeeper-3.4.14.tar.gz
RUN tar -zxf zookeeper-3.4.14.tar.gz && \
    mkdir -p /var/zookeeper/data
COPY zoo.cfg /opt/zookeeper-3.4.14/conf/zoo.cfg
ENV PATH="/opt/zookeeper-3.4.14/bin:${PATH}"
EXPOSE 2181 2888 3888
CMD zkServer.sh start-foreground
    
    
