Introduction
This README provides step-by-step instructions for setting up an AWS EMR (Elastic MapReduce) cluster to run Hadoop and Spark applications. The process involves initializing the cluster, creating input and output directories within the Hadoop Distributed File System (HDFS), and executing a JAR file using Spark.

Step 1: AWS EMR Cluster Initialization
Log in to your AWS Management Console.

Navigate to the AWS EMR service.


Step 2: Create HDFS Directories
SSH into your AWS EMR cluster using the provided credentials.

Use the Hadoop command to create an input and output directory in HDFS:

hdfs dfs -mkdir hdfs:///input
hdfs dfs -mkdir hdfs:///output

Step 3: Execute the JAR File using Spark
Upload your JAR file to the AWS EMR cluster. You can use scp or any other method to transfer the JAR file to the cluster.

Use the following Spark command to execute the JAR file:

spark-submit --master local --class streaming.assinment3  assignment-3.jar hdfs:///input  hdfs:///output

assinment3 is the object written in scala for assignment 3 and its jar file is assignment-3.jar. 
the 1st argument (meaning hdfs:///input) is the folder to monitor and the 2nd argument (hdfs:///output) is the output path.
Monitor the execution and wait for it to complete.

Step 4: Verify Output
After the JAR file execution is complete, check the output directory for the following information:

outputpath/TaskA-number
outputpath/TaskB-number
outputpath/TaskC-number