giraph-ADS
==========

implementation of facility location on Giraph

Change the value of $k$ (used in bottom k) in line 41 of the FacilityLocationADSWeighted.java

Command used:

hadoop dfs -rmr output_weighted_london_20;mvn compile -e;time hadoop jar <path/to/>/giraph-ADS/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeighted -vif org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeightedInputFormat -vip input_weighted/input_ADS_london.txt -of org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeightedOutputFormat -op output_weighted_london_20 giraph.yarn.task.heap.mb 2046 -w 1
