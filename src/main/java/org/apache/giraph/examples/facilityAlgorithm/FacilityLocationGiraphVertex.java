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

import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.graph.Vertex;
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
	public double distanceStepSize = 100;
	
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
	
	private double getNeighborhoodSize(String ADS, Map<Double,Double> frozenClients, double distance) {
		double neighborhoodSize = 0.0;
		Map<Double,Double> currentADS = new TreeMap<Double, Double>();
		
		String[] tmp = ADS.split(";");
		for(int i=0;i<tmp.length;i++) { // first filter out only the non frozen clients
			String[] tmp1 = tmp[i].split(":");
			double key = Double.parseDouble(tmp1[0]);
			double value = Double.parseDouble(tmp1[1]);
			if(!frozenClients.containsKey(key)) {
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
		
		double alpha = getAggregatedValue(DIST_ALPHA);
		double facilityCost = getValue().getFacilityCost();
		Map<Double, Double> frozenClients = getAggregatedValue(FROZEN_CLIENTS);
		
		Map<Double, String> vertexADS = LoadADSFromFile();
		
		double vertexId = getId().get(), neighborhoodSize = 0.0, t_i = 0.0;
		String ADS = vertexADS.get(vertexId);
		
		for(int i=0; i<alpha; i+=distanceStepSize) {
			neighborhoodSize = getNeighborhoodSize(ADS,frozenClients,i); // only consider those nodes that are not frozen in the i-neighborhood.
			t_i += neighborhoodSize * ((1 + FacilityLocationGiraphMasterCompute.EPS)*alpha - i) - max(0, (alpha-i));
		}
		
		if(t_i >= facilityCost) { // open facility
			getValue().setIsFacilityOpen();
			getValue().setAlphaAtFacilityOpen(alpha);
		}
		
	}

	

	
	
}