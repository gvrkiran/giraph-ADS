package org.apache.giraph.examples.facilityAlgorithm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

// giraph implementation of facility location algorithm
public class FacilityLocationGiraphVertex extends
Vertex<LongWritable, FacilityLocationGiraphVertexValue,FloatWritable, DoublePairWritable> {

	String ADSFile = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/output_ADS_london_10.txt";
	String IdToHashMapping = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/input_ADS_london.txt";
	public static int ADS_BOTTOM_K = 10;
	
	public static String DIST_ALPHA = "distanceAlpha";
	public static String FROZEN_CLIENTS = "frozenClients";
	public static String OPEN_FACILITIES = "openFacilities";
	public static String PHASE = "phase"; // contains which function to run
	public double distanceStepSize = 1; // 1 for un-weighted, SET accordingly for weighted case.
	
	private Map<Double,String> LoadADSFromFile() throws NumberFormatException, IOException { // load ADS from file
		BufferedReader br = new BufferedReader(new FileReader(IdToHashMapping));
		String line = null;
		String[] line_split = null;
		Map<Double,Double> idToHash = new HashMap<Double,Double>();
		Map<Double,String> vertexADS = new HashMap<Double,String>();
		
		while ((line = br.readLine()) != null) {
			line_split = line.split("\t");
			try {
				idToHash.put(Double.parseDouble(line_split[1]),Double.parseDouble(line_split[0]));				
			} catch(ArrayIndexOutOfBoundsException e) {
				System.out.println("Something wrong in the input file: " + e);
			}
		}
		
		br.close();
		BufferedReader br1 = new BufferedReader(new FileReader(IdToHashMapping));
		while ((line = br1.readLine()) != null) {
			line_split = line.split("\t");
			double id = Double.parseDouble(line_split[0]);
			String[] line_split1 = line_split[1].split(";");
			String out = "";
			for(int i=0;i<line_split1.length;i++) {
				String[] tmp = line_split1[i].split(":");
				String key = idToHash.get(Double.parseDouble(tmp[0])).toString() + ":" + tmp[1];				
				out += key + ";";
			}
			vertexADS.put(id,out);
		}
		
		br1.close();
		return vertexADS;
	}
	
	private double getNeighborhoodSize(String ADS, Set<Double> frozenClients, double distance) {
		double neighborhoodSize = 0.0;
		Map<Double,Double> currentADS = new TreeMap<Double, Double>();
		
		String[] tmp = ADS.split(";");
		for(int i=0;i<tmp.length;i++) { // first filter out only the non frozen clients
			String[] tmp1 = tmp[i].split(":");
			double key = Double.parseDouble(tmp1[0]);
			double value = Double.parseDouble(tmp1[1]);
			if(!frozenClients.contains(key)) {
				currentADS.put(key,value);
			}
		}
		
		List<Double> vertexADS1 = (List<Double>) currentADS.keySet();
		if(vertexADS1.size()>ADS_BOTTOM_K) {
			neighborhoodSize = (ADS_BOTTOM_K-1)/currentADS.get(vertexADS1.get(ADS_BOTTOM_K));
		}
		else {
			neighborhoodSize = vertexADS1.size();
		}
		
		return neighborhoodSize;
	}
	
	private double max(int num1, double num2) {
		if(num1>num2)
			return num1;
		else
			return num2;
	}
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) throws IOException {
		
		double alpha = this.<DoubleWritable>getAggregatedValue(DIST_ALPHA).get();
		boolean phase = this.<BooleanWritable>getAggregatedValue(PHASE).get();
		double facilityCost = getValue().getFacilityCost();
		double vertexId = getId().get();
		double t_i = getValue().getTi();
		
		Set<Double> frozenClients = this.<MapWritable>getAggregatedValue(FROZEN_CLIENTS).get();
		Set<Double> openFacilities = this.<MapWritable>getAggregatedValue(OPEN_FACILITIES).get();
		
		Set<Double> receivedFreezeMessagesFrom = getValue().getReceivedFreezeMessagesFrom(); // add vertices which have already been seen
		
		Map<Double, String> vertexADS = LoadADSFromFile();
		
		if(phase==true) { // run computation to open facilities
			double neighborhoodSize = 0.0;
			String ADS = vertexADS.get(vertexId);
		
			for(int i=0; i<alpha; i+=distanceStepSize) {
				neighborhoodSize = getNeighborhoodSize(ADS,frozenClients,i); // only consider those nodes that are not frozen in the i-neighborhood.
				t_i += neighborhoodSize * ((1 + FacilityLocationGiraphMasterCompute.EPS)*alpha - i) - max(0, (alpha-i));
			}
			
			getValue().setTi(t_i);
		
			if(t_i >= facilityCost) { // open facility
				getValue().setIsFacilityOpen();
				getValue().setAlphaAtFacilityOpen(alpha);
				aggregate(PHASE, new BooleanWritable(false));
				openFacilities.add(vertexId);
			}
		}
		else { // run method to send freeze messages
			
			double maxDist = -1000000, maxId = -1;
			
			double remaining_distance = 0, id = 0;
			long superStepNum = getSuperstep();
			if(superStepNum==0) {
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(getId().get(), alpha);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
			
			for (DoublePairWritable message: messages) { // if the vertex receives multiple messages, only propagate the one with the highest remaining distance
				id = message.getFirst();
				remaining_distance = message.getSecond();
				
				/*
				if(maxDist < remaining_distance) {
					maxDist = remaining_distance;
					maxId = id;
				}
				*/
			
				if(receivedFreezeMessagesFrom.contains(id)==false && id!=getId().get()) { // if the vertex already hasnt received a message from this id.. and self loop 
					receivedFreezeMessagesFrom.add(id);
					frozenClients.add((double) getId().get());
					aggregate(FROZEN_CLIENTS,new MapWritable().getMapWritable(frozenClients));
					remaining_distance = maxDist - 1; // un-weighted case
					if(remaining_distance>=0) {
						aggregate(PHASE, new BooleanWritable(false));
						for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
							DoublePairWritable dpw = new DoublePairWritable(id, remaining_distance);
							sendMessage(edge.getTargetVertexId(), dpw);
						}
					}
					else {
						aggregate(PHASE, new BooleanWritable(true));
						voteToHalt();
					}
				}
			}
		}
		
	}	
	
}