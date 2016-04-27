#!/bin/sh

wget -nc -P /vagrant/ http://apache.mirrors.spacedump.net/hadoop/common/stable/hadoop-2.7.2.tar.gz

echo EXTRACTING HADOOP
tar -zxf /vagrant/hadoop-2.7.2.tar.gz -C .

echo Exporting ENV. Variables
cat /vagrant/config/.bashrc > /home/vagrant/.bashrc
source "/vagrant/config/.bashrc"

sudo chown -R vagrant:vagrant $HADOOP_PREFIX/
sudo chmod -R 775 $HADOOP_PREFIX/

sudo cp /vagrant/config/core-site.xml $HADOOP_PREFIX/etc/hadoop/core-site.xml
sudo cp /vagrant/config/hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
sudo cp /vagrant/config/yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# Overwrite the 127.0.0.1 <hostname> <hostname>
sudo cp /vagrant/config/hosts /etc/hosts

sudo rm -rf $HADOOP_PREFIX/hdfs/
