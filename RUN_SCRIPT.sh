NUM_VERTICES=500
BOTTOM_K=20
FLAG=1 # 0=unweighted, 1=weighted
GRAPHTYPE="forestfire";

# generate a random graph using Forest fire

cd /users/kiran/Downloads/snap/examples/forestfire
./forestfire -o:graph.txt -n:$NUM_VERTICES -f:0.3 -b:0.25

cd -

# format the generated graph into a format usable

if [ $FLAG -eq 0 ]; then
	cat /users/kiran/Downloads/snap/examples/forestfire/graph.txt | python ADSTest/makeDirectedForestFireGraph.py > automated_data/graphs/graph_$GRAPHTYPE\_$NUM_VERTICES.txt;
else
	cat /users/kiran/Downloads/snap/examples/forestfire/graph.txt | python ADSTestWeighted/makeDirectedForestFireGraph.py > automated_data/graphs/graph_$GRAPHTYPE\_$NUM_VERTICES.txt;
fi

# first compute the input for ADS (could be weighted or unweighted) for the given graph.

if [ $FLAG -eq 0 ]; then
	python ADSTest/generateADSInput.py automated_data/graphs/graph_$GRAPHTYPE\_$NUM_VERTICES.txt > automated_data/input_ADS/input_$GRAPHTYPE\_$NUM_VERTICES.txt;
else
	python ADSTestWeighted/generateADSInput.py automated_data/graphs/graph_$GRAPHTYPE\_$NUM_VERTICES.txt > automated_data/input_ADS/input_$GRAPHTYPE\_$NUM_VERTICES.txt;
fi

# copy input to HDFS

if [ $FLAG -eq 0 ]; then
	hadoop dfs -rm input_ADS_unweighted/*;
	hadoop dfs -copyFromLocal automated_data/input_ADS/input_$GRAPHTYPE\_$NUM_VERTICES.txt input_ADS_unweighted/;
else
	hadoop dfs -rm input_ADS_weighted/*;
	hadoop dfs -copyFromLocal automated_data/input_ADS/input_$GRAPHTYPE\_$NUM_VERTICES.txt input_ADS_weighted/;
fi

# Run ADS computation Giraph program

# unweighted
if [ $FLAG -eq 0 ]; then
	hadoop dfs -rmr output_ADS_unweighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K;mvn compile -e;time hadoop jar /users/kiran/workspace/giraph-test32/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.ads.FacilityLocationADS -vif org.apache.giraph.examples.ads.FacilityLocationADSInputFormat -vip input_ADS_unweighted/input_$GRAPHTYPE\_$NUM_VERTICES.txt -of org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeightedOutputFormat -op output_ADS_unweighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K -ca FacilityLocationADS.bottom_k=$BOTTOM_K giraph.yarn.task.heap.mb 2046 -w 1;
	hadoop dfs -cat output_ADS_unweighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K/* > automated_data/ADS/output_ADS_$NUM_VERTICES\_$BOTTOM_K.txt
else
	hadoop dfs -rmr output_ADS_weighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K;mvn compile -e;time hadoop jar /users/kiran/workspace/giraph-test32/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeighted -vif org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeightedInputFormat -vip input_ADS_weighted/input_$GRAPHTYPE\_$NUM_VERTICES.txt -of org.apache.giraph.examples.adsWeighted.FacilityLocationADSWeightedOutputFormat -op output_ADS_weighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K -ca FacilityLocationADSWeighted.bottom_k=$BOTTOM_K giraph.yarn.task.heap.mb 2046 -w 1;
	hadoop dfs -cat output_ADS_weighted/output_ADS_$NUM_VERTICES\_$BOTTOM_K/* > automated_data/ADS/output_ADS_$NUM_VERTICES\_$BOTTOM_K.txt
fi

# compute input for Facility Location Algorithm
python facilityAlgorithmTest/generateFacilityAlgorithmInput.py automated_data/graphs/graph_$GRAPHTYPE\_$NUM_VERTICES.txt> automated_data/input_facility/input_facility_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt;

# copy that file to HDFS
hadoop dfs -rm input_facility/*;
hadoop dfs -copyFromLocal automated_data/input_facility/input_facility_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt input_facility/;

# Run the actual Facility Location Giraph program
hadoop dfs -rmr output_facility;mvn compile -e;time hadoop jar /users/kiran/workspace/giraph-test32/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphVertex -vif org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphInputFormat -vip input_facility/input_facility_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt -of org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphOutputFormat -op output_facility -mc org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphMasterCompute -ca FacilityLocationGiraphVertex.bottom_k=$BOTTOM_K -ca FacilityLocationGiraphVertex.ADSFile="/users/kiran/workspace/giraph-test32/automated_data/ADS/output_ADS_"$NUM_VERTICES"_"$BOTTOM_K".txt" -ca FacilityLocationGiraphVertex.IdToHashMapping="/users/kiran/workspace/giraph-test32/automated_data/input_ADS/input_"$GRAPHTYPE"_"$NUM_VERTICES".txt" -ca FacilityLocationGiraphVertex.weightedFlag=$FLAG giraph.yarn.task.heap.mb 2046 -w 1;

# copy output to disk
hadoop dfs -cat output_facility/* > automated_data/output_facility/output_facility_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt;

# generate input for Lubys
python LubysTest/generateLubysInput.py automated_data/output_facility/output_facility_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt > automated_data/input_lubys/input_lubys_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt;

# copy to HDFS
hadoop dfs -rm input_lubys/*;
hadoop dfs -copyFromLocal automated_data/input_lubys/input_lubys_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt input_lubys;

# Run the Lubys algorithm Giraph program
hadoop dfs -rmr output_lubys;mvn compile -e;time hadoop jar /users/kiran/workspace/giraph-test32/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.luby.LubysAlgorithm -vif org.apache.giraph.examples.luby.LubysAlgorithmInputFormat -vip input_lubys/input_lubys_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt -of org.apache.giraph.examples.luby.LubysAlgorithmOutputFormat -op output_lubys -mc org.apache.giraph.examples.luby.LubysAlgorithmMasterCompute giraph.yarn.task.heap.mb 2046 -w 1

hadoop dfs -cat output_lubys/* > automated_data/output_lubys/output_lubys_$GRAPHTYPE\_$NUM_VERTICES\_$BOTTOM_K.txt;
