echo Formatting NameNode

## Start HDFS daemons
# Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
$HADOOP_PREFIX/bin/hdfs namenode -format -force -nonInteractive

# Because formating makes a folder under @root.
sudo chown -R vagrant:vagrant $HADOOP_PREFIX/
sudo chmod -R 775 $HADOOP_PREFIX/