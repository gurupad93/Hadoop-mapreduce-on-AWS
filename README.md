# Hadoop-mapreduce-on-AWS

Steps followed:

1.  Create Maven project in eclipse.
2.  Update dependencied in pom.xml. This will include all the JAR files required for the project.
    In this case, hadoop-client libraries are needed.
3.  Create Java classes to include mapper and reducer code.
4.  Run as java application to check if it is error free.
5.  Export it as runnable JAR file with all the required libraries in extracted jar.

6.  Create AWS account.
7.  Create S3 bucket to store the files.
8.  Upload input files(10 copies) and JAR file created in step 5 to s3 bucket.
9.  Create EC2 key pair. 
10. Create cluster in EMR. Choose path of s3 bucket created in previous step. Choose the EMR release.
    In applications check Core hadoop option. Make sure the hadoop versions here and the one in pom.xml are same.
11. Choose instance type in hardware configuration. For this project m4.large is choosen. Number of instances are 4 (1 master     and 3 core nodes).
12. Choose EC2 key pair created in step 9 in security section. Permissions are default. Click on create cluster.
13. It takes some time to setup the instances. Wait till the Running status are shown for Master and Core nodes.
14. Go to steps tab. Click on Add step. In JAR location, enter the s3 path of the JAR file. In arguments box, enter the           arguments like s3 path of input files, output directory. Click on Add.
15. Monitor the status and can view the output files in s3 once status changes from Running to Completed.
16. Terminate the instances once the run is complete.


Tasks Implemented:

1.  Counting the number of occurences of each word.
2.  Counting the number of occurences of 2 adjacent words.
3.  In input files, count the number of occurences of every word which are present in other file(word-patterns.txt in this         case). This is done using Distributed caching feature of hadoop. 
   
