package org.apache.giraph.examples.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

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
	public static final IntConfOption CLEANUP_FREQ = new IntConfOption("FacilityLocationADS.cleanUPFreq",2);
	
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(FacilityLocationADS.class);

	// private Vertex<LongWritable, FacilityLocationADSVertexValue, FloatWritable, DoublePairWritable> vertex;
	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	private Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	
	public static Map<Double, Double> CleanUP1(Map<Double, Double> vertexADS, int bottom_k) {
		// for each d, keep only the bottom k entries in the ADS
		
		Map<Double, List<Double>> tmpMap = new HashMap<Double, List<Double>>();
		Map<Double, List<Double>> tmpMapCleaned = new HashMap<Double, List<Double>>();
		Map<Double, Double> finalADS = new HashMap<Double, Double> ();
		List<Double> tmpList = new ArrayList<Double> ();
		List<Double> tmpList1 = new ArrayList<Double> ();
		List<Double> tmpList2 = new ArrayList<Double> ();
		List<Double> tmpList3 = new ArrayList<Double> ();
		List<Double> tmpListPrev = new ArrayList<Double> ();
		List<Double> tmpListAdd = new ArrayList<Double> ();

		double maxDist = -1;
		
		for (Map.Entry<Double, Double> entry : vertexADS.entrySet()) {
			double hash = entry.getKey();
			double distance = entry.getValue();
			
			if(distance>maxDist) {
				maxDist = distance;
			}
			
			if(tmpMap.containsKey(distance)) {
				tmpList = tmpMap.get(distance);
				tmpList.add(hash);
				tmpMap.put(distance,tmpList);
			}
			else {
				tmpList = new ArrayList<Double> ();
				tmpList.add(hash);
				tmpMap.put(distance,tmpList);
			}
		}
		
		for (double i=0; i<maxDist; i++) {
			tmpList3 = new ArrayList<Double> ();
			if(tmpMap.containsKey(i)) {
				tmpList1 = tmpMap.get(i);
				// System.out.println("TmpList1 PREV size " + tmpList1.size());
				tmpListAdd.addAll(tmpList1);
				tmpListAdd.addAll(tmpListPrev);
				// System.out.println("PREV Tmplist size" + tmpList1.size() + " tmplistADD size " + tmpListAdd.size());
				Collections.sort(tmpListAdd);
				if(tmpListAdd.size()<bottom_k) {
					tmpList2 = tmpListAdd;
				}
				else {
					tmpList2 = tmpListAdd.subList(0, bottom_k); // get the top k	
				}
				
				for(int k=0; k<tmpList2.size(); k++) {
					double vv = tmpList2.get(k);
					if(vertexADS.get(vv)==i) { // only retain those which correspond to this dist, i
						tmpList3.add(vv);
					}
				}
				
				// System.out.println(tmpList2.size() + "," + tmpList3.size());
				// System.out.println("PREV Tmp Map size before " + tmpMap.get(i).size());
				tmpMapCleaned.put(i,tmpList3);
				// System.out.println("PREV Tmp Map size after " + tmpMapCleaned.get(i).size());
				tmpListPrev = tmpList1;
				// System.out.println("Tmp list PREV after i= " + i + " tmpListADD size "
					//				+ tmpListAdd.size());
			}
		}
		
		for (Map.Entry<Double, List<Double>> entry : tmpMapCleaned.entrySet()) {
			List<Double> tmpList4 = new ArrayList<Double>();
			tmpList4 = entry.getValue();
			
			for(int j=0; j<tmpList4.size(); j++) {
				finalADS.put(tmpList4.get(j), entry.getKey());
			}
		}
		
		if(finalADS.size()==0) { // dont clean up if you are returning an empty ADS
			return vertexADS;
		}
		else
			return finalADS;
	}
	
	public static Map<Double, Double> CleanUP(Map<Double, Double> vertexADS, int bottom_k) {
		// for each d, keep only the bottom k entries in the ADS
		
		Map<Double, List<Double>> tmpMap = new HashMap<Double, List<Double>>();
		Map<Double, Double> finalADS = new HashMap<Double, Double> ();
		List<Double> tmpList = new ArrayList<Double> ();
		List<Double> tmpList1 = new ArrayList<Double> ();
		List<Double> tmpListAdd = new ArrayList<Double> ();
		
		double maxDist = -1, current_length;
		
		for (Map.Entry<Double, Double> entry : vertexADS.entrySet()) {
			double hash = entry.getKey();
			double distance = entry.getValue();
			
			if(distance>maxDist) {
				maxDist = distance;
			}
			
			if(tmpMap.containsKey(distance)) {
				tmpList = tmpMap.get(distance);
				tmpList.add(hash);
				tmpMap.put(distance,tmpList);
			}
			else {
				tmpList = new ArrayList<Double> ();
				tmpList.add(hash);
				tmpMap.put(distance,tmpList);
			}
		}
		
		for (double i=0; i<maxDist; i++) {
			
			if(tmpMap.containsKey(i)) {
				tmpList = tmpMap.get(i);
				tmpListAdd.addAll(tmpList);
				tmpListAdd.addAll(tmpList1);
				// System.out.println("Tmplist size" + tmpList.size() + " tmplistADD size " + tmpListAdd.size());
				Collections.sort(tmpListAdd);
				
				if(tmpListAdd.size()<bottom_k) {
					current_length = tmpListAdd.size();
				}
				else {
					current_length = bottom_k; // get the top k	
				}
				
				List<Double> tmpList2 = new ArrayList<Double> ();
				for(int k=0; k<current_length; k++) {
					double vv = tmpListAdd.get(k);
					if(vertexADS.get(vv)==i) { // only retain those which correspond to this dist, i
						tmpList2.add(vv);
					}
				}
				
				/*
				System.out.println("Tmp Map size before " + tmpMap.get(i).size());
				tmpMapCleaned.put(i,tmpList2);
				System.out.println("Tmp Map size after " + tmpMapCleaned.get(i).size());
				tmpList1 = tmpList;
				*/
				for(int j=0; j<tmpList2.size(); j++) {
					finalADS.put(tmpList2.get(j), i);
				}
			}
		}
		
		/*
		for (Map.Entry<Double, List<Double>> entry : tmpMapCleaned.entrySet()) {
			List<Double> tmpList3 = new ArrayList<Double>();
			tmpList3 = entry.getValue();
			
			for(int j=0; j<tmpList3.size(); j++) {
				finalADS.put(tmpList3.get(j), entry.getKey());
			}
		}
		*/
		
		if(finalADS.size()==0) {// dont clean up if you are returning an empty ADS
			return vertexADS;
		}
		else
			return finalADS;
	}
	
	@Override
	public void compute(
			Iterable<DoublePairWritable> messages) {

		int bottom_k = BOTTOM_K.get(getConf());
		int cleanUPFreq = CLEANUP_FREQ.get(getConf());
		vertexADS = getValue().getADS();
		
		// System.out.println("Superstep " + getSuperstep() + " vertex id " + vertex.getId().toString() + " bottom_k " + bottom_k);
		// System.out.println("ADS " + vertexADS.keySet());
		// System.out.println("Superstep " + getSuperstep() + " vertex id " + getId().toString() + " Size " + vertexADS.size() + " Bottom k " + bottom_k);
		
		// Set<Double> temp = vertexADS.keySet();
		// List<Double> vertexADS1 = new ArrayList<Double>(temp);
		
		List<Double> vertexADS1 = new ArrayList<Double>(vertexADS.keySet());
		
		Collections.sort(vertexADS1);
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
		
		if(getSuperstep()%cleanUPFreq == 0) { // cleanup the ADS after every 10 supersteps, by only keeping the bottom-k
			if(getSuperstep()!=0) {
				// System.out.println("Cleanup (regular) in Superstep " + getSuperstep() + " Size of ADS before cleanup " + vertexADS.size());
				vertexADS = CleanUP(vertexADS, bottom_k);
				// System.out.println("Size of ADS after cleanup " + vertexADS.size());
				// System.out.println("Size of ADS after cleanup1 " + CleanUP1(vertexADS,bottom_k).size());
				// System.gc(); // cleanup the memory from all the tmpLists
			}
		}

		for (DoublePairWritable message : messages) {
			// System.out.println("Message " + message.toString());
			float addToADS = 0;
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
			
			if(addToADS==1) {
			// if(hash<hashAtK) {
				// System.out.println("Came to vertex: " + getId().get() + " ADS SIZE: " + vertexADS.size() + " add to ADS " + addToADS + " hash " + hash + " distance " + distance);
				distance += 1.0;
				if(vertexADS.containsKey(hash)==false) {
					vertexADS.put(hash,distance);
					
					if(vertexADS.size()>15000) { // some big number, CHANGE
						// System.out.println("Cleanup in Superstep " + getSuperstep() + " for vertex " + getId().get() 
						//			+ " Size of ADS before cleanup " + vertexADS.size());
						vertexADS = CleanUP(vertexADS, bottom_k);
						
						// System.out.println("Size of ADS after cleanup " + CleanUP(vertexADS, bottom_k).size());
						// System.out.println("Size of ADS after cleanup1 " + CleanUP1(vertexADS,bottom_k).size());
						/*
						if(tmp.size()==vertexADS.size()) {
							vertexADS = CleanUP1(vertexADS, bottom_k);
						}
						else {
							vertexADS = tmp;
						}
						*/
						// System.gc(); // cleanup the memory from all the tmpLists
						// System.out.println("Size of ADS after cleanup " + vertexADS.size());
					}
					
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