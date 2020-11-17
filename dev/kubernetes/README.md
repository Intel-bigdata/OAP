# OAP Dockerfile & Scripts for running on Kubernetes
This directory contains the docker file and useful scripts to help user to build docker images for Spark & OAP running on Kubernetes.

## Build Spark/OAP Docker Image
Before doing this, make sure you have installed docker at your machine.

### Build the Spark Base Docker Image
Go to folder docker/spark-centos and execute the following command to build Spark base docker image.
``` 
docker build --tag spark-centos:1.0.0 .
``` 

### Build the OAP Docker Image
Go to folder docker/oap-centos and execute the following command to build OAP docker image which is based on Spark base docker image.
``` 
docker build --tag oap-centos:0.9.0 .
``` 

## Run Spark/OAP on Kubernetes
Before doing this, we assume you have setup Kubernetes enironment and it worked properly. All the tool scripts are under "spark" folder. 
We tested these scripts in Minikube environment. If you are using other Kubernetes distributions, you may need to make some changes to work properly.

### Create Spark User and Assign Cluster Role
Spark running on Kubernetes needs edit role of your Kubernetes cluster to create driver or executor pods. 
Go to spark folder and execute the following command to create "spark" user and assign the role. Make sure you have logged in Kubernetes and have administor role of the cluster.
``` 
sh ./spark-kubernetes-prepare.sh
``` 

### Run Spark/OAP Job in Cluster mode
In Kubernetes, you can run Spark/OAP job using spark-submit in Cluster mode at any node which has access to your Kubernetes API server.

#### Run Spark Pi Job
You can run a Spark Pi job for a simple testing of the enironment is working. Execute the following command. If you are running on the master node,  you can ignore the --master parameter.
For example:
``` 
sh ./spark-pi.sh --master localhost:8443  --image oap-centos:0.9.0  --spark_conf ./conf
``` 
#### Run Spark Job through spark-submit
You can submit your own job. Execute the following command. If you are running on the master node,  you can ignore the --master parameter.
For exmaple:
``` 
sh ./spark-submit.sh --master localhost:8443  --image oap-centos:0.9.0  --spark_conf ./conf --name spark-pi --class org.apache.spark.examples.SparkPi  local:///opt/home/spark-3.0.0/examples/jars/spark-examples_2.12-3.0.0.jar 100
``` 

### Run Spark/OAP in Client Mode
A lot of Spark tools run at Client Mode, such Spark Thrift Server, Spark Shell and Spark SQL. Spark Submit can also run at Client Mode.

#### Spark Thrift Server

##### Start Spark Thrift Server
Execute the following command to start Spark Thrift Server in a pod and launch corresponding services.
For example:
``` 
sh ./spark-thrift-server.sh start --image oap-centos:0.9.0  --spark_conf ./conf
``` 

##### Stop Spark Thrift Server
Execute the following command to stop Spark Thrift Server in a pod and stop corresponding services.
``` 
sh ./spark-thrift-server.sh stop
``` 
##### Connect beeline to Spark Thrift Server
Execute the following command to connect to the Thrift server. Make sure you execute it on one of your Kubernetes cluster node.
``` 
sh ./spark-beeline.sh
``` 

##### Launch a Shell to Spark Thrift Server
Execute the following command to launch a command shell to the server.
``` 
sh ./spark-thrift-server.sh client
``` 

#### Run Other Spark Client Mode Tools
To run Spark Shell, Spark SQL or Spark Submit at client mode, we need to launch a client pod and then run correponding tool in the pod.

##### Start the Client
Execute the following command to configure and start the client pod.
For example:
``` 
sh ./spark-client.sh start --image oap-centos:0.9.0 --spark_conf ./conf
``` 

##### Run Spark Shell
After you have started the client pod, you can execute the following command to start the Spark shell into the pod:
For example:
``` 
sh ./spark-shell-client.sh --conf spark.executor.instances=1
``` 

##### Run Spark SQL
After you have started the client pod, you can execute the following command to start the Spark SQL into the pod:
For example:
``` 
sh ./spark-sql-client.sh --conf spark.executor.instances=1
``` 

##### Run Spark Submit
After you have started the client pod, you can execute the following command to start the Spark Submit into the pod.
For example:
``` 
sh ./spark-submit-client.sh --conf spark.executor.instances=1 --name spark-pi --class org.apache.spark.examples.SparkPi  local:///opt/home/spark-3.0.0/examples/jars/spark-examples_2.12-3.0.0.jar 100
``` 

##### Stop the Client
If you have completed all your work, execute the following command to stop the client pod.
``` 
sh ./spark-client.sh stop
``` 
