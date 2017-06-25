hadoop fs -rm /user/mhasnaoui/testForMarwen/logFact-0.0.1-SNAPSHOT-jar-with-dependencies.jar
hadoop fs -put logFact-0.0.1-SNAPSHOT-jar-with-dependencies.jar /user/mhasnaoui/testForMarwen
hadoop fs -rm /user/mhasnaoui/testForMarwen/log4j-spark1.properties
hadoop fs -put log4j-spark1.properties /user/mhasnaoui/testForMarwen

spark-submit --master yarn --deploy-mode cluster --conf spark.network.timeout=10000000 \
--conf spark.executor.heartbeatInterval=10000000 --conf spark.yarn.driver.memoryOverhead=3000 --conf spark.yarn.executor.memoryOverhead=5120 --num-executors 5 --executor-cores 4 --executor-memory 10g \
--driver-memory 5g \
--files hdfs:///user/mhasnaoui/testForMarwen/log4j-spark1.properties \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-spark1.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j-spark1.properties" \
--class sparkTest.logFact.Main hdfs://ws44-nn1.tl.teralab-datascience.fr:8020/user/mhasnaoui/testForMarwen/logFact-0.0.1-SNAPSHOT-jar-with-dependencies.jarspark-submit --master spark://mbp-de-marwen.home:7077  --driver-java-options "-Dlog4j.configuration=file:///tmp/log4j.properties" --files /tmp/log4j.properties --class sparkTest.logFact.Main logFact-0.0.1-SNAPSHOT.jar
