package org.apache.giraph.examples.adsLubys;

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
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.examples.ads.FacilityLocationADSVertexValue;
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
	
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(FacilityLocationADS.class);

	// private Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	// private Map<Double, TreeSet<Double>> vertexADSTmp = new HashMap<Double, TreeSet<Double>> ();
	
	/*
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
	
	static class PQsort implements Comparator<Double> {
		 
		public int compare(Double one, Double two) {
			return two > one ? 1 : -1;
		}
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
				
				for(int j=0; j<tmpList2.size(); j++) {
					finalADS.put(tmpList2.get(j), i);
				}
			}
		}
		
		
		if(finalADS.size()==0) {// dont clean up if you are returning an empty ADS
			return vertexADS;
		}
		else
			return finalADS;
	}
	*/
	
	public ArrayList<Double> addCustom(ArrayList<Double> list1, double element1, int bottom_k) {
		
		if(list1.size()<bottom_k) {
			list1.add(element1);
			Collections.sort(list1);
			return list1;
		}
		
		else {
			
			Collections.sort(list1);
			if(list1.get(list1.size()-1)>element1) {
				list1.remove(list1.size()-1); // remove the kth element
				Set<Double> set1 = new HashSet<Double>(list1);
				set1.add(element1);
				list1 = new ArrayList<Double>(set1);
				// System.out.println("HEreee " + list1.size());
			}
			
			Collections.sort(list1);
			// list1 = (ArrayList<Double>) list1.subList(0, bottom_k-1);
				
			return list1;
		}
		// return list1;
	}
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) {

		int bottom_k = BOTTOM_K.get(getConf());
		// int cleanUPFreq = CLEANUP_FREQ.get(getConf());
		// int numMachines = NUM_MACHINES.get(getConf());
		Map<Double, TreeSet<Double>> vertexADSTmp = getValue().getADSTmp();
		double currentDistance = getValue().getCurrentDistance();
		
		// TreeSet<Double> tmpList1 = vertexADSTmp.get(currentDistance);
		TreeSet<Double> tmpList1 = new TreeSet<Double> ();
		currentDistance += 1; // increase by 1 hop in each superstep
		
		if(getSuperstep()>3)
			voteToHalt();

		
		// System.out.println("Superstep " + getSuperstep() + " currentDistance " + currentDistance);
		// vertexADS = getValue().getADS();

		// PQsort pqs = new PQsort();
		// PriorityQueue<Double> priorityQueue = new PriorityQueue<Double> (bottom_k, pqs); // a priority queue to keep the bottom k
			
		// System.out.println("ADS " + vertexADS.keySet());
		// System.out.println("Superstep " + getSuperstep() + " vertex id " + getId().toString() + " Size " + vertexADS.size() + " Bottom k " + bottom_k);
		
		// Set<Double> temp = vertexADS.keySet();
		// List<Double> vertexADS1 = new ArrayList<Double>(temp);
		
		// List<Double> vertexADS1 = new ArrayList<Double>(vertexADS.keySet());
		
		// Collections.sort(vertexADS1);
		
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
			// System.out.println("Vertex id " + getId().get() + " in machine " + getId().get()%numMachines);
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				// System.out.println("Edge " + edge.getTargetVertexId());
				// DoublePairWritable dpw = new DoublePairWritable(hashAtK,0.0);
				DoublePairWritable dpw = new DoublePairWritable(getValue().getHashValue(),currentDistance);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		/*
		if(getSuperstep()%cleanUPFreq == 0) { // cleanup the ADS after every 10 supersteps, by only keeping the bottom-k
			if(getSuperstep()!=0) {
				// System.out.println("Cleanup (regular) in Superstep " + getSuperstep() + " Size of ADS before cleanup " + vertexADS.size());
				// vertexADS = CleanUP(vertexADS, bottom_k);
				// System.out.println("Size of ADS after cleanup " + vertexADS.size());
				// System.out.println("Size of ADS after cleanup1 " + CleanUP1(vertexADS,bottom_k).size());
				// System.gc(); // cleanup the memory from all the tmpLists
			}
		}
		*/

		float addToADS = 0;
		double hash;
		for (DoublePairWritable message : messages) {
			// System.out.println("Message " + message.toString());
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
			
			/*
			// Collections.sort(tmpList1);
			ArrayList<Double> tmpList2 = tmpList1;
			// System.out.println("TmpList before " + tmpList1 + " hash " + hash);
			tmpList1 = addCustom(tmpList1, hash, bottom_k);
			// System.out.println("TmpList after " + tmpList1 + " hash " + hash);
			
			if(tmpList1.size()>=bottom_k) { // if the received entry is less than the kth entry in the ADS
				// if(tmpList2.get(tmpList2.size()-1)<=hash) {
				if(gVar==1) {
					gVar = 0;
					addToADS = 1;
					set1 = new HashSet<Double>(tmpList2);
				}
			}
			else { // if the ADS is still not of size k
				addToADS = 1;
				set1 = new HashSet<Double>(tmpList2);
			}
			*/
			
			// System.out.println("Superstep " + getSuperstep() + " Vertex Id " + getId().get() + " addToADS " + addToADS + " tmpList size " + tmpList1.size());
			
		}
		
		Iterator<Double> iterator = tmpList1.iterator();
		while(iterator.hasNext()) {
			hash = iterator.next();
			// if(addToADS==1) { // && set1.contains(hash)==false) {// if the hash was added to the ADS
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) { // send (r,d+1) to its neighbors
					DoublePairWritable dpw = new DoublePairWritable(hash,currentDistance);
					// System.out.println("Sending message to " + edge.getTargetVertexId() + " message " + dpw.toString());
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
		// }
		
		// if(addToADS==1) {// update ADS
			// if(hash<hashAtK) {
				// System.out.println("Came to vertex: " + getId().get() + " ADS SIZE: " + vertexADS.size() + " add to ADS " + addToADS + " hash " + hash + " distance " + distance);
				// distance += 1.0;
		getValue().setCurrentDistance(currentDistance);
		vertexADSTmp.put(currentDistance,tmpList1);
		getValue().setADSTmp(vertexADSTmp);
				/*
				if(vertexADS.containsKey(hash)==false) {
					vertexADS.put(hash,currentDistance);
					
					if(vertexADS.size()>15000) { // some big number, CHANGE
						// System.out.println("Cleanup in Superstep " + getSuperstep() + " for vertex " + getId().get() 
						//			+ " Size of ADS before cleanup " + vertexADS.size());
						vertexADS = CleanUP(vertexADS, bottom_k);
						
						// System.out.println("Size of ADS after cleanup " + CleanUP(vertexADS, bottom_k).size());
						// System.out.println("Size of ADS after cleanup1 " + CleanUP1(vertexADS,bottom_k).size());
						// System.gc(); // cleanup the memory from all the tmpLists
						// System.out.println("Size of ADS after cleanup " + vertexADS.size());
					}
				*/	
				// }
			// }
		if(getSuperstep()!=0) {
			// System.out.println("Vertex id " + getId().get() + " voted to halt");
			voteToHalt();
		}
	}
}