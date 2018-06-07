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
pip3 install flask==0.12.2
pip3 install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" http://package.mapr.com/releases/MEP/MEP-4.0.0/mac/mapr-streams-python-0.9.2.tar.gz
