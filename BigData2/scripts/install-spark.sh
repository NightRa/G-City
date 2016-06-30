echo "Downloading Spark..."
wget -nc -P /vagrant/ http://www.motorlogy.com/apache/spark/spark-1.6.2/spark-1.6.2-bin-without-hadoop.tgz 2> /dev/null

echo EXTRACTING Spark
tar -zxf /vagrant/spark-1.6.2-bin-without-hadoop.tgz -C .
