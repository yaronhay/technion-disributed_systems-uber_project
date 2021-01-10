FROM ubuntu:latest


ENV TZ=Asia/Jerusalem
RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install -y iproute2 telnet net-tools \
 && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
 && apt-get install -y tzdata \
 && apt-get clean


RUN apt-get install openjdk-11-jdk -y


ADD zookeeper/zk_install.sh /usr/local/bin/
RUN apt-get install wget -y \
 && chmod +x /usr/local/bin/zk_install.sh \
 && /usr/local/bin/zk_install.sh



RUN apt-get update \
 && apt-get install curl build-essential -y \
 && curl -sL https://deb.nodesource.com/setup_14.x | bash \
 && apt-get install nodejs



ADD zookeeper/zk_start.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/zk_start.sh


ENTRYPOINT ["/usr/local/bin/zk_start.sh", "1"]
