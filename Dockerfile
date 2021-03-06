FROM java:openjdk-8u111-jdk

RUN apt-get update -qq && apt-get install -y wget git

RUN wget http://mirrors.sonic.net/apache/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz && \
       tar -zxf apache-maven-3.5.2-bin.tar.gz && rm apache-maven-3.5.2-bin.tar.gz && \
       mv apache-maven-3.5.2 /usr/local && ln -s /usr/local/apache-maven-3.5.2/bin/mvn /usr/bin/mvn

RUN mkdir /app

WORKDIR /app

ADD . /app

RUN mvn clean package

ARG mainClass

ENV mainClass $mainClass

CMD mvn exec:java -Dexec.mainClass=$mainClass
