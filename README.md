# Counting Indexer (EE 382N: Homework 3)
In this problem, you will implement a counting indexer using Hadoop, a programming
model and software framework for developing applications that concurrently process large scale data
(Big Data) on distributed systems. The indexer you develop has to identify the file and calculate
the occurrences of every word that appear in the given set of les. The data set for this problem is
the novel Pride and Prejudice. A collection of les (one per chapter) of all the text is provided in
the attached zip le for the assignment. Your implementation must be able to run in Single-
Node mode using Hadoop 2.6.5.

* Project requirement and setup instructions:
    *  *f17hw3.pdf* question 3
* Start Hadoop daemons: 
	* *hadoop-daemon.sh start namenode*
	* *hadoop-daemon.sh start datanode*
	* *hadoop-daemon.sh start secondarynamenode*
	* *yarn-daemon.sh start resourcemanager*
	* *yarn-daemon.sh start nodemanager*
	* *mr-jobhistory-daemon.sh start historyserver*
* Copy the local input directory to HDFS input directory
    * *hdfs dfs -copyFromLocal input/ /input*
* List / remove the files in the directory /output on HDFS
    * *hdfs dfs -ls /output*
    * *hdfs dfs -rm -r /output*
* Run hadoop:
    * *hadoop jar CountingIndexer.jar CountingIndexer /input /output*
* Copy output result from HDFS to local
    * *hdfs dfs -copyToLocal /output*
* Screenshots:
    * *results/screenshots.pdf*
* Results:
    * Job 1: *results/outputTemp*
    * Job 2: *results/output*