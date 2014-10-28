package org.apache.giraph.examples.ads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * Demonstrates the basic Pregel ADS computation for neighborhood computation.
 */
@Algorithm(
		name = "Facility Location unweighted",
		description = "Facility location for unweighted graphs using All distances sketches (ADS)."
		)
public class FacilityLocationADS extends
	Vertex<LongWritable, FacilityLocationADSVertexValue,FloatWritable, DoublePairWritable> {
	/** the number of elements to consider in the bottom k sketch $k$ */
	public static final IntConfOption BOTTOM_K = 
			new IntConfOption("FacilityLocationADS.bottom_k", 20);
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(FacilityLocationADS.class);

	private Vertex<LongWritable, FacilityLocationADSVertexValue, FloatWritable, DoublePairWritable> vertex;
	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	private Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) {

		int bottom_k = BOTTOM_K.get(getConf());
		vertexADS = getValue().getADS();
		System.out.println("Superstep " + getSuperstep() + " vertex id " + getId().toString() + " bottom_k " + bottom_k);
		System.out.println("ADS " + vertexADS.keySet());
		// System.out.println("Superstep " + getSuperstep() + " vertex id " + getId().toString() + " Size " + vertexADS.size() + " Bottom k " + bottom_k);
		
		Set<Double> temp = vertexADS.keySet();
		List<Double> vertexADS1 = new ArrayList<Double>(temp);
		
		Collections.sort(vertexADS1);
		double hashAtK = 0;
		// if(vertexADS1.size()==0)
		//	voteToHalt();

		/*
		if(vertexADS1.size()>=bottom_k)
			hashAtK = vertexADS1.get(bottom_k-1); // get the kth hash value
		else {
			hashAtK = vertexADS1.get(vertexADS1.size()-1); // if the ADS does not consist of bottom_k elements yet, return the last element
			// hashAtK = Double.MAX_VALUE;
		}
		*/

		if (getSuperstep() == 0) {
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				// System.out.println("Edge " + edge.getTargetVertexId());
				// DoublePairWritable dpw = new DoublePairWritable(hashAtK,0.0);
				DoublePairWritable dpw = new DoublePairWritable(vertexADS1.get(0),0.0);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}

		for (DoublePairWritable message : messages) {
			// System.out.println("Message " + message.toString());
			double addToADS = 0;
			double hash = message.getFirst();
			double distance = message.getSecond();

			if(vertexADS1.size()>=bottom_k) { // if the received entry is less than the kth entry in the ADS
				if(vertexADS1.get(bottom_k-1)>hash) {
					addToADS = 1;
				}
			}
			else { // if the ADS is still not of size k
				addToADS = 1;
			}
			if(addToADS==1d) {
			// if(hash<hashAtK) {
				// System.out.println("Came to vertex: " + getId().get() + " ADS SIZE: " + vertexADS.size() + " add to ADS " + addToADS + " hash " + hash + " distance " + distance);
				distance += 1.0;
				if(vertexADS.containsKey(hash)==false) {
					vertexADS.put(hash,distance);
					for (Edge<LongWritable, FloatWritable> edge : getEdges()) { // send (r,d+1) to its neighbors
						DoublePairWritable dpw = new DoublePairWritable(hash,distance);
						// System.out.println("Sending message to " + edge.getTargetVertexId() + " message " + dpw.toString());
						sendMessage(edge.getTargetVertexId(), dpw);
					}
				}
			}
		}

		voteToHalt();
	}
}