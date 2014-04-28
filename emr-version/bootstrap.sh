#! /bin/bash
 
#debian R upgrade

gpg --keyserver pgpkeys.mit.edu --recv-key 06F90DE5381BA480
gpg -a --export 06F90DE5381BA480 | sudo apt-key add -
echo "deb http://streaming.stat.iastate.edu/CRAN/bin/linux/debian squeeze-cran/" | sudo tee -a /etc/apt/sources.list
sudo apt-get update
sudo apt-get -t squeeze-cran install --yes --force-yes r-base r-base-dev

set -e

sudo curl -o Rcpp.tar.gz        http://cran.us.r-project.org/src/contrib/Archive/Rcpp/Rcpp_0.10.2.tar.gz 
sudo curl -o RJSONIO.tar.gz     http://cran.us.r-project.org/src/contrib/Archive/RJSONIO/RJSONIO_1.0-1.tar.gz 
sudo curl -o digest.tar.gz	    http://cran.us.r-project.org/src/contrib/Archive/digest/digest_0.6.2.tar.gz 
sudo curl -o functional.tar.gz	http://cran.us.r-project.org/src/contrib/Archive/functional/functional_0.1.tar.gz 
sudo curl -o plyr.tar.gz        http://cran.us.r-project.org/src/contrib/Archive/plyr/plyr_1.8.tar.gz 
sudo curl -o stringr.tar.gz     http://cran.us.r-project.org/src/contrib/Archive/stringr/stringr_0.6.tar.gz 
sudo curl -o rJava.tar.gz       http://cran.us.r-project.org/src/contrib/Archive/rJava/rJava_0.9-4.tar.gz 
sudo curl -o bitops.tar.gz      http://cran.us.r-project.org/src/contrib/Archive/bitops/bitops_1.0-5.tar.gz 
sudo curl -o reshape2.tar.gz    http://cran.us.r-project.org/src/contrib/Archive/reshape2/reshape2_1.2.tar.gz 
sudo curl -o caTools.tar.gz     http://cran.us.r-project.org/src/contrib/Archive/caTools/caTools_1.14.tar.gz 

sudo R CMD INSTALL Rcpp.tar.gz
sudo R CMD INSTALL RJSONIO.tar.gz
sudo R CMD INSTALL digest.tar.gz
sudo R CMD INSTALL functional.tar.gz
sudo R CMD INSTALL plyr.tar.gz
sudo R CMD INSTALL stringr.tar.gz
sudo R CMD INSTALL rJava.tar.gz
sudo R CMD INSTALL bitops.tar.gz
sudo R CMD INSTALL reshape2.tar.gz
sudo R CMD INSTALL caTools.tar.gz

sudo curl --insecure -L http://raw.github.com/RevolutionAnalytics/rmr2/master/build/rmr2_2.3.0.tar.gz | tar zx
sudo R CMD INSTALL --byte-compile rmr2


sudo su << EOF1
echo '
export HADOOP_HOME=/home/hadoop
export HADOOP_CMD=/home/hadoop/bin/hadoop
export HADOOP_STREAMING=/home/hadoop/contrib/streaming/hadoop-streaming.jar
export JAVA_HOME=/usr/lib64/jvm/java-7-oracle/

' >> /etc/profile

EOF1

sudo su << EOF2
echo '
export JAVA_HOME=/usr/lib64/jvm/java-7-oracle/
' >> /home/hadoop/conf/hadoop-env.sh

EOF2
