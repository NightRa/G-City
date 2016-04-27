echo "Starting Master NameNode & ResourceManager"
hadoop-daemon.sh start namenode
yarn-daemon.sh start resourcemanager

echo "Starting Master DataNode & NodeManager"
hadoop-daemon.sh start datanode
yarn-daemon.sh start nodemanager