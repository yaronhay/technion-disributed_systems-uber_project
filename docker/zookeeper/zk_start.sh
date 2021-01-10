#!/bin/bash

# Constants
P1="2181"
P2="2888"
P3="3888"

# Go to ZK home
cd /opt/zookeeper || exit 1

function getIP() {
  ip_inet="$(ip -4 addr show scope global dev eth0 | grep inet)"
  echo "$ip_inet" | awk '{print $2}' | cut -d / -f 1
}

# Definitions
HOSTNAME=$(hostname)
ipaddr=$(getIP)

# Arguments
id=$1
other_zk_node=$2

function zk_start() {
    ZOO_LOG_DIR=/var/log \
    ZOO_LOG4J_PROP='INFO,CONSOLE,ROLLINGFILE' \
    ./bin/zkServer.sh $1
}

# Create the first node
function init_zk_cluster() {
  # Config Server
  local srv_cfg="server.$id=$ipaddr:$P2:$P3;$P1"
  echo "$srv_cfg" >> ./conf/zoo.cfg.dynamic

  # Initialize Zookeeper
  ./bin/zkServer-initialize.sh --force --myid="$id"

  # Start Zookeeper Server
  zk_start start-foreground

}

# Create a node that joins the cluster
function join_cluster() {
  # Add other nodes to the to the dynamic configuration file
  local srv_cfg="$(bin/zkCli.sh -server "$other_zk_node:$P1" get /zookeeper/config | grep ^server)"
  echo  "$srv_cfg" >> ./conf/zoo.cfg.dynamic

  # Add the info of the current node
  local current_node_info="server.$id=$ipaddr:$P2:$P3:observer;$P1"
  echo "$current_node_info" >> ./conf/zoo.cfg.dynamic

  cp ./conf/zoo.cfg.dynamic ./conf/zoo.cfg.dynamic.org

  # Initialize the node and start it
  ./bin/zkServer-initialize.sh --force --myid="$id"
  zk_start start

  # Ask the existing cluster to reconfigure, and
  # add the information of the current server
  current_node_info="server.$id=$ipaddr:$P2:$P3:participant;$P1"
  ./bin/zkCli.sh -server "$other_zk_node:$P1" reconfig -add "$current_node_info"

  # Stop Zookeeper on the current node and start it again in the foreground,
  # so our container will keep running
  ./bin/zkServer.sh stop
  zk_start start-foreground

}

if [ -n "$other_zk_node" ]  # len($other_zk_node) != 0
then
  join_cluster
else                        # len($other_zk_node) == 0
  init_zk_cluster
fi
