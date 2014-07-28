package org.apache.giraph.examples.freeze;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

public class FacilityLocationSendFreezeMessagesMasterCompute extends DefaultMasterCompute{
	
	double alpha = 1.0; // some initial value CHANGE to gamma/(m^2 * (1+eps))
	double EPS = 0.02; // some value. CHANGE later
	int numFrozenClients = 0; // number of frozen facilities
	int numOpenFacilities = 0; // number of open facilities
	Map<Double, String > vertexADS = new HashMap<Double, String>();
	int numVertices = 0; // after readADSData, we can assign this the value of the number of vertices
	
	String ADSFile = "/tmp/output_ADS.txt";
	String IdToHashMapping = "/tmp/input_ADS.txt";
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		try {
			vertexADS = readADSData(); // read ADS data from disk
			numVertices = vertexADS.size();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		registerPersistentAggregator(FacilityLocationSendFreezeMessages.DIST_ALPHA, DoubleSumAggregator.class);
    }
	
	@Override
	public void compute() {
		// double alpha = getAggregatedValue(FacilityLocationSendFreezeMessages.DIST_ALPHA);
		double alpha = 0;
		alpha = alpha * (1 + EPS);
		setAggregatedValue(FacilityLocationSendFreezeMessages.DIST_ALPHA, new DoubleWritable(alpha));
		
		// use ADS to get the alpha-neighborhood of a node 
		int n_ij = 10; // CHANGE
		
		// Open facilities which have f(n_ij) > f_i and run sendFreezeMessages() routine for those
		numOpenFacilities += 1;
		
		if(getSuperstep()>10) {
			haltComputation();
		}
			
	}
	
	private Map<Double, String> readADSData() throws IOException { // read ADS data from disk
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
}
