package org.apache.giraph.examples.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.giraph.conf.AbstractConfOption;
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

import com.google.common.collect.Lists;

/**
 * Demonstrates the basic Pregel ADS computation for neighborhood computation.
 */
@Algorithm(
		name = "Facility Location unweighted",
		description = "Facility location for unweighted graphs using All distances sketches (ADS)."
		)
public class FacilityLocationADS extends
	Vertex<LongWritable, FacilityLocationADSVertexValue, FloatWritable, DoublePairWritable> {
	/** the number of elements to consider in the bottom k sketch $k$ */
	public static final IntConfOption BOTTOM_K = 
			new IntConfOption("FacilityLocationADS.bottom_k", 20);
	// public static final IntConfOption CLEANUP_FREQ = new IntConfOption("FacilityLocationADS.cleanUPFreq",2);
	// public static final IntConfOption NUM_MACHINES = new IntConfOption("FacilityLocationADS.numMachines",100);
	public static final IntConfOption MAX_DISTANCE = new IntConfOption("FacilityLocationADS.maxDistance",10);
	
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(FacilityLocationADS.class);

	// private Map<Double, TreeSet<Double>> vertexADSTmp = new HashMap<Double, TreeSet<Double>> ();
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) {

		int bottom_k = BOTTOM_K.get(getConf());
		int maxDistance = MAX_DISTANCE.get(getConf());
		Map<Double, TreeSet<Double>> vertexADSTmp = getValue().getADSTmp();
		double currentDistance = getValue().getCurrentDistance();
		
		if(currentDistance>maxDistance)
			voteToHalt();
		
		// TreeSet<Double> tmpList1 = vertexADSTmp.get(currentDistance);
		TreeSet<Double> tmpList1 = new TreeSet<Double> ();
		currentDistance += 1; // increase by 1 hop in each superstep
		
		if (getSuperstep() == 0) {
			// System.out.println("Vertex id " + getId().get() + " in machine " + getId().get()%numMachines);
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getValue().getHashValue(),currentDistance);
				// System.out.println("Sending message from " + getId().get() + " to " + edge.getTargetVertexId() 
						// + " hash " + getValue().getHashValue() + " in superstep " + getSuperstep() + " distance " + currentDistance);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		float addToADS = 0;
		double hash = 0;
		// Set<Double> set1 = new HashSet<Double> ();
		for (DoublePairWritable message : messages) {
			hash = message.getFirst();
			// double distance = message.getSecond();
			addToADS = 0;
			
			if(tmpList1.size()>=bottom_k) {
				if(tmpList1.last()>hash) {
					if(tmpList1.contains(hash)==false) {
						addToADS = 1;
						tmpList1.pollLast();
						tmpList1.add(hash);
					}
				}
			}
			
			else {
				if(tmpList1.contains(hash)==false) {
					tmpList1.add(hash);
					addToADS = 1;
				}
			}
		}
		
			// if(addToADS==1) { // && set1.contains(hash)==false) {// if the hash was added to the ADS
		Iterator<Double> iterator = tmpList1.iterator();
		while(iterator.hasNext()) {
			hash = iterator.next();
		
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) { // send (r,d+1) to its neighbors
					DoublePairWritable dpw = new DoublePairWritable(hash,currentDistance);
					// System.out.println("Sending message to " + edge.getTargetVertexId() + " message " + dpw.toString());
					// System.out.println("Sending message from " + getId().get() + " to " + edge.getTargetVertexId()
								// + " hash " + hash + " in superstep " + getSuperstep() + " distance " + currentDistance
								// + " tmpList " + tmpList1);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
		// }
		
		getValue().setCurrentDistance(currentDistance);
		vertexADSTmp.put(currentDistance,tmpList1);
		getValue().setADSTmp(vertexADSTmp);

		if(getSuperstep()!=0) {
			// System.out.println("Vertex id " + getId().get() + " voted to halt");
			voteToHalt();
		}
	}
}
