#!/bin/sh

if [ -d "/usr/lib/jvm/" ]; then
        echo "There's already an installation of Java JDK in /usr/lib/jvm"
        echo "Skipping..."
        exit 0
fi

sudo apt-get install curl -y

if [ -f /vagrant/jdk-8-linux-x64.tar.gz ]; then   
    echo Using Cached JVM Archive    
else 
	curl -L --cookie "oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.tar.gz -o /vagrant/jdk-8-linux-x64.tar.gz
fi

echo "Extractivg the JDK..."
tar -xf /vagrant/jdk-8-linux-x64.tar.gz

sudo mkdir -p /usr/lib/jvm
sudo mv ./jdk1.8.* /usr/lib/jvm/

sudo update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_77/bin/java" 1
sudo update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/jvm/jdk1.8.0_77/bin/javac" 1
sudo update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/lib/jvm/jdk1.8.0_77/bin/javaws" 1

sudo chmod a+x /usr/bin/java
sudo chmod a+x /usr/bin/javac
sudo chmod a+x /usr/bin/javaws
sudo chown -R root:root /usr/lib/jvm/jdk1.8.0_77

# rm jdk-8-linux-x64.tar.gz
rm -f equip_base.sh
rm -f equip_java7_64.sh

java -version
