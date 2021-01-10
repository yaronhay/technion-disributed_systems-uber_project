#!/bin/sh

cd /opt

wget https://downloads.apache.org/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz

tar -xvf apache-zookeeper-3.6.2-bin.tar.gz
mv apache-zookeeper-3.6.2-bin zookeeper

cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg
echo "standaloneEnabled=false" >> /opt/zookeeper/conf/zoo.cfg
echo "dynamicConfigFile=/opt/zookeeper/conf/zoo.cfg.dynamic" >> /opt/zookeeper/conf/zoo.cfg


