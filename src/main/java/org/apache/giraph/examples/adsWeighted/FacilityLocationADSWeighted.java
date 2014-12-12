package org.apache.giraph.examples.adsWeighted;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import org.apache.giraph.examples.ads.DoublePairWritable;

/**
 * Demonstrates the basic Pregel ADS computation for neighborhood computation.
 */
@Algorithm(
		name = "Facility Location weighted",
		description = "Facility location for unweighted graphs using All distances sketches (ADS)."
		)
public class FacilityLocationADSWeighted extends
	Vertex<LongWritable, FacilityLocationADSWeightedVertexValue, FloatWritable, DoublePairWritable> {
	/** the number of elements to consider in the bottom k sketch $k$ */
	public static final IntConfOption BOTTOM_K = 
			new IntConfOption("FacilityLocationADSWeighted.bottom_k", 10);
	public static final IntConfOption MAX_ITERATIONS = new IntConfOption("FacilityLocationADSWeighted.maxIterations",10);
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(FacilityLocationADSWeighted.class);

	// private Vertex<LongWritable, FacilityLocationADSWeightedVertexValue, FloatWritable, DoublePairWritable> vertex;
	// private Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	// private Map<Double, Double> prevIterAdded = new HashMap<Double, Double>();
	
	@SuppressWarnings("unchecked")
	private static HashMap<Double, Double> sortByValues(Map<Double, Double> tempADS) { 
		List list = new LinkedList(tempADS.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		} 
		return sortedHashMap;
	}

	/*
	private static Map sortByComparator(Map unsortMap) {
 
		List list = new LinkedList(unsortMap.entrySet());
 
		// sort list based on comparator
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
                                       .compareTo(((Map.Entry) (o2)).getValue());
			}
		});
 
		// put sorted list into map again
                //LinkedHashMap make sure order in which keys were inserted
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	*/
	
	private List<Double> getElementsCloser(Map<Double, Double> vertexADS, double distance) {
		
		List<Double> vertexADS1 = new ArrayList<Double>();
		
		for(double key: vertexADS.keySet()) {
			if(vertexADS.get(key)<distance)
				vertexADS1.add(key);
		}
		
		Collections.sort(vertexADS1);
		return vertexADS1;
	}
	
	/*
	private List<Double> getElementsCloser1(Map<Double, Double> vertexADS, double distance) {
		
		List<Double> vertexADS1 = new ArrayList<Double>();
		
		for(double key: vertexADS.keySet()) {
			if(vertexADS.get(key)<distance)
				vertexADS1.add(key);
		}
		
		Collections.sort(vertexADS1);
		return vertexADS1;
	}
	*/
	
	private Map<Double, Double> CleanUP(Map<Double, Double> vertexADS, double distance, int bottom_k) {
		
		List<Double> vertexADS1 = new ArrayList<Double>();
		Map<Double, Double> tempADS = new HashMap<Double, Double>();
		Map<Double, Double> tempADS1 = new HashMap<Double, Double>();
		
		vertexADS = sortByValues(vertexADS); // so that we can scan (x,y) by increasing y
		// vertexADS = sortByComparator(vertexADS); // so that we can scan (x,y) by increasing y
		
		for(Iterator<Map.Entry<Double, Double>>it=vertexADS.entrySet().iterator();it.hasNext();){
			Map.Entry<Double, Double> entry = it.next();
			double currentKey = entry.getKey();
			double currentValue = entry.getValue();
			
			if(currentValue<=distance) {
				continue;
			}
			vertexADS1 = getElementsCloser(vertexADS,currentValue);
			// System.out.println("In CleanUP " + vertexADS1);
			
			if(vertexADS1.size() < bottom_k) {
				continue;
			}
			
			else if(currentKey>vertexADS1.get(bottom_k-1)) {
					it.remove();
			}
		}
		
		/*
		tempADS = sortByValues(vertexADS);
		for(double key: tempADS.keySet()) {
			if(tempADS.get(key)<=distance) {
				tempADS1.put(key,vertexADS.get(key));
				continue;
			}
			
			vertexADS1 = getElementsCloser(vertexADS,tempADS.get(key));
			
			if(vertexADS1.size() < bottom_k) {
				tempADS1.put(key,vertexADS.get(key));
			}
			else {
				if(key<=vertexADS1.get(bottom_k-1)) {
					tempADS1.put(key,vertexADS.get(key));
				}
			}
		}
		*/
		
		return vertexADS;
	}
	
	private Map<Double, Double> emptyPrevIterAdded(Map<Double, Double> prevIterAdded) { // return an empty hashmap
		
		for(Iterator<Map.Entry<Double, Double>>it = prevIterAdded.entrySet().iterator();it.hasNext();){
			Entry<Double, Double> entry = it.next();
			it.remove();
		}
		
		return prevIterAdded;
	}
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) {

		int bottom_k = BOTTOM_K.get(getConf());
		int maxIterations = MAX_ITERATIONS.get(getConf());
		Map<Double, Double> prevIterAdded = getValue().getPrevIterAdded();
		// Map<Double, Double> vertexADS = getValue().getADS();
		Map<Double, Map<Double, Double>> vertexADSTmp = getValue().getADSTmp();
		
		double currentIteration = getValue().getCurrentIteration();
		
		Map<Double, Double> vertexADS = new HashMap<Double, Double> (); //vertexADSTmp.get(currentIteration);
		// Map<Double, Double> vertexADS = vertexADSTmp.get(currentIteration);
		List<Double> vertexADS1 = new ArrayList<Double> ();
		currentIteration += 1;
		
		// System.out.println("Superstep " + getSuperstep() + " vertex id " + getId().get()
			//		+ " currentIteration " + currentIteration + " vertexADS size " + vertexADS.size()
			// 		+ " prevIterAdded size " + prevIterAdded.size());
		// System.out.println("ADS " + vertexADS.keySet() + " Prev iter added " + prevIterAdded.keySet());
		
		// if(prevIterAdded.size()==0)
			// voteToHalt();
		
		/*
		Set<Double> temp = vertexADS.keySet();
		List<Double> vertexADS1 = new ArrayList<Double>(temp); // contains ADS sorted by hash
		
		Collections.sort(vertexADS1);
		*/
		
		if (getSuperstep() == 0) {
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getValue().getHashValue(),edge.getValue().get());
				// System.out.println("Sending message (0th superstep) on Edge " + getId().toString() + " " + edge.getTargetVertexId());
				// DoublePairWritable dpw = new DoublePairritable(vertexADS1.get(0),0.0,edge.getTargetVertexId().get());
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		double addToADS = 0, hash, distance;
		// send updates
		
		if(getSuperstep() != 0) {
			for (Entry<Double, Double> entry : prevIterAdded.entrySet()) { // for all those elements added in the previous iteration
				hash = entry.getKey();
				distance = entry.getValue();
			
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					distance = entry.getValue() + edge.getValue().get();
					DoublePairWritable dpw = new DoublePairWritable(hash,distance);
					// DoublePairADSWritable dpw = new DoublePairADSWritable(hashReceived,distance,getId().get(),0,vertexADS);
					// System.out.println("Sending hash " + hash + " from " + getId().get() + " to " + edge.getTargetVertexId());
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}

			prevIterAdded = emptyPrevIterAdded(prevIterAdded); // delete everything from the map that was added in the previous iterations
			// prevIterAdded = new HashMap<Double, Double>();
		}
		
		// process updates
		for (DoublePairWritable message : messages) {
			addToADS = 0;
			hash = message.getFirst();
			distance = message.getSecond();

			vertexADS1 = getElementsCloser(vertexADS,distance); // first get all elements closer in the ADS than the current distance and then look at the kth element
			
			if(vertexADS1.size()>=bottom_k) {
				if(vertexADS1.get(bottom_k-1)>hash) {
					addToADS = 1;
				}
			}
			else {
				addToADS = 1;
			}

			// System.out.println("Received Message from " + hash + " in vertex " + getId().get() + " addToADS " + addToADS);
			
			if(addToADS==1) {
			// if(hash<hashAtK) {
				// distance += 1.0;
				if(vertexADS.containsKey(hash)==false) {
					vertexADS.put(hash,distance);
					prevIterAdded.put(hash,distance);
					// System.out.println("Came to vertex: " + getId().get() + " ADS SIZE: " + vertexADS.size() + " prevIterAdded " + prevIterAdded.size() + " hash " + hash + " distance " + distance);
					// Clean Up
					// System.out.println("ADS size before CleanUP " + vertexADS.size());
					// vertexADS = CleanUP(vertexADS,distance,bottom_k);
					// long endTime = System.nanoTime();
					// long duration = endTime - startTime;
					// System.out.println("ADS size after CleanUP " + vertexADS.size() + " . Time taken " + duration);
					// System.out.println("Prev Iter added " + prevIterAdded.keySet());
				}
			}
		}

		// System.out.println("ADS size " + vertexADS.size() + " prevIterAdded size " + prevIterAdded.size());
		getValue().setCurrentIteration(currentIteration);
		vertexADSTmp.put(currentIteration,vertexADS);
		getValue().setADSTmp(vertexADSTmp);
		// getValue().setADS(vertexADS);
		getValue().setPrevIterAdded(prevIterAdded);
		// System.out.println("Node id " + vertex.getId().get() + " voted to halt");
		if(getSuperstep()!=0) {
			voteToHalt();
		}
	}
}