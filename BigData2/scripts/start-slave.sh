echo "### Starting Slave DataNode & NodeManager"

hadoop-daemon.sh start datanode
yarn-daemon.sh start nodemanager