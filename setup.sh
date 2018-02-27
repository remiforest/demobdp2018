yum update -y
yum install -y java-1.8.0-openjdk-devel
wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
yum install -y apache-maven
yum install -y epel-release
yum install -y mapr-librdkafka
yum install -y librdkafka-devel
yum install -y python34
yum install -y python34-setuptools
yum install -y python34-devel.x86_64
easy_install-3.4 pip
yum install -y gcc-c++
pip3 install maprdb
