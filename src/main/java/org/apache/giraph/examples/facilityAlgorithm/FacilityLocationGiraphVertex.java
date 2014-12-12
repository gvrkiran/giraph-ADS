package org.apache.giraph.examples.facilityAlgorithm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.RandomWalkWorkerContext;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.base.Joiner;

// giraph implementation of facility location algorithm
public class FacilityLocationGiraphVertex extends
Vertex<LongWritable, FacilityLocationGiraphVertexValue, FloatWritable, DoublePairWritable> {

	// String ADSFile = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/output_ADS_5.txt";
	// String IdToHashMapping = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/input_ADS.txt";
	// String ADSFile = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/output_ADS_flight_5.txt";
	// String IdToHashMapping = "/users/kiran/workspace/giraph-test32/ADSTestWeighted/input_ADS_flight.txt";
	
	public static final StrConfOption ADS_FILE = new StrConfOption("FacilityLocationGiraphVertex.ADSFile", "");
	public static final StrConfOption ID_TO_HASH_MAPPING = new StrConfOption("FacilityLocationGiraphVertex.IdToHashMapping", "");
	public static final IntConfOption ADS_BOTTOM_K = new IntConfOption("FacilityLocationGiraphVertex.bottom_k", 5);
	public static final StrConfOption WEIGHTED_FLAG = new StrConfOption("FacilityLocationGiraphVertex.weightedFlag",""); // 0=unweighted, 1=weighted
	public static final FloatConfOption EPSILON = new FloatConfOption("FacilityLocationGiraphVertex.EPS",0.2f);
	public static final IntConfOption NUM_MACHINES = new IntConfOption("FacilityLocationGiraphVertex.numMachines",100);
	// public static int ADS_BOTTOM_K = 5;
	
	public static String MAX_AGG_GAMMA = "maxGamma"; // contains the maximum value of facility cost at superstep 0.
	public static String DIST_ALPHA = "distanceAlpha";
	public static String FROZEN_CLIENTS = "frozenClients";
	public static String OPEN_FACILITIES = "openFacilities";
	public static String PHASE = "phase"; // contains which function to run
	public static String PHASE_SWITCH = "phaseSwitch"; // aggregator to decide if we have to switch the phase
	public static float EPS = 0.2f;
	
	Map<Double, Double> vertexMapping = new HashMap<Double, Double>();
	int flag_freeze = 0;
	String ADS = null;
	
	private Map<Double,String> LoadADSFromFile(String ADSFile, String IdToHashMapping) throws NumberFormatException, IOException { // load ADS from file
		
		/*
		String ADSFileFolder = "";
		String [] ADSFileFolder1 = ADSFile.split("/");
		for(int i=0;i<ADSFileFolder1.length-1;i++) {
			ADSFileFolder += ADSFileFolder1[i] + "/";
		}
		
		Configuration conf = getContext().getConfiguration();
		Path sourceFile = null;
		System.out.println("ADS File folder " + ADSFileFolder);
		sourceFile = DistributedCacheUtils.getLocalCacheFile(conf, ADSFileFolder).get();
		
		FileSystem fs = FileSystem.getLocal(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(
            fs.open(sourceFile), Charset.defaultCharset()));
		*/
		
        /*
        FileSystem fs = null;
		try {
			fs = FileSystem.get( new URI(ADSFileFolder), conf);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        LocalFileSystem localFileSystem = fs.getLocal(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(localFileSystem.open(pt)));
		*/
        
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
		
		/*
		String IdToHashMappingFolder = "";
		String [] IdToHashMappingFolder1 = IdToHashMapping.split("/");
		for(int i=0;i<IdToHashMappingFolder1.length-1;i++) {
			IdToHashMappingFolder += IdToHashMappingFolder1[i] + "/";
		}
		
		Configuration conf1 = getContext().getConfiguration();
		Path sourceFile1 = null;
		sourceFile1 = DistributedCacheUtils.getLocalCacheFile(conf1, IdToHashMappingFolder).get();
		
		FileSystem fs1 = FileSystem.getLocal(conf1);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(
            fs1.open(sourceFile1), Charset.defaultCharset()));
        */
        
        /*
		Configuration conf1 = new Configuration();
        Path pt1 = new Path(IdToHashMapping);
        FileSystem fs1 = null;
		try {
			fs1 = FileSystem.get( new URI(IdToHashMappingFolder), conf1);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        LocalFileSystem localFileSystem1 = fs1.getLocal(conf1);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(localFileSystem1.open(pt1)));
		*/
        
		BufferedReader br1 = new BufferedReader(new FileReader(ADSFile));
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
	
	private double getNeighborhoodSize(String ADS, Set<Double> frozenClients, double distance, int bottom_k) {
		double neighborhoodSize = 0.0;
		Map<Double,Double> currentADS = new TreeMap<Double, Double>();
		
		String[] tmp = ADS.split(";");
		for(int i=0;i<tmp.length;i++) { // first filter out only the non frozen clients
			String[] tmp1 = tmp[i].split(":");
			if(tmp1.length!=2)
				continue;
			double key = Double.parseDouble(tmp1[0]);
			key = vertexMapping.get(key);
			double value = Double.parseDouble(tmp1[1]);
			if(!frozenClients.contains(key) && value<=distance) {
				currentADS.put(key,value);
			}
		}
		
		// List<Double> vertexADS1 = (List<Double>) currentADS.keySet();
		List<Double> vertexADS1 = new ArrayList<Double>(currentADS.keySet());
		// System.out.println("ADS actual size " + tmp.length + " frozen clients " + frozenClients.size() + " ADS current size " + vertexADS1.size());
		if(vertexADS1.size()>bottom_k) {
			neighborhoodSize = (bottom_k-1)/currentADS.get(vertexADS1.get(bottom_k));
		}
		else {
			neighborhoodSize = vertexADS1.size();
		}
		
		if(Double.isInfinite(neighborhoodSize))
			neighborhoodSize = 0;
		
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
		
		// System.out.println("Came here in superstep " + getSuperstep() + " vertex id " + getId().get() + " flag_freeze" + flag_freeze);
		
		int bottom_k = ADS_BOTTOM_K.get(getConf());
		
		// System.out.println("Vertex ID " + getId().get() + " ADS size " + getValue().getADS().split(";").length);
		// String ADSFile = ADS_FILE.get(getConf());		
		// String IdToHashMapping = ID_TO_HASH_MAPPING.get(getConf());
		String weightedFlag = WEIGHTED_FLAG.get(getConf());
		EPS = EPSILON.get(getConf());
		
		double distanceStepSize = 0;
		
		if(weightedFlag=="0") {
			distanceStepSize = 1;
		}
		else {
			distanceStepSize = Math.round(1 + EPS); // for weighted case.
		}
		// public double distanceStepSize = 1; // 1 for un-weighted, SET accordingly for weighted case.
		
		
		double alpha = this.<DoubleWritable>getAggregatedValue(DIST_ALPHA).get();
		boolean phase = this.<BooleanWritable>getAggregatedValue(PHASE).get();
		
		double facilityCost = getValue().getFacilityCost();
		double vertexId = getId().get();
		double t_i = getValue().getTi();
		
		// System.out.println("Parameters: bottom_k " + bottom_k + " ADSFile " + ADSFile + " IdToHashMapping " + IdToHashMapping + " WeightedFlag " + weightedFlag);
	
		/*
		Set<Double> frozenClients = new HashSet<Double>();
		Set<Double> openFacilities = new HashSet<Double>();
		Set<Double> receivedFreezeMessagesFrom = new HashSet<Double>();
		*/
		
		Set<Double> frozenClients = this.<MapWritable>getAggregatedValue(FROZEN_CLIENTS).get();
		Set<Double> openFacilities = this.<MapWritable>getAggregatedValue(OPEN_FACILITIES).get();
		Set<Double> phaseSwitch = this.<MapWritable>getAggregatedValue(PHASE_SWITCH).get();
		
		Set<Double> receivedFreezeMessagesFrom = getValue().getReceivedFreezeMessagesFrom(); // add vertices which have already been seen
		
		// System.out.println("Came here in compute alpha " + alpha + " phase " + phase + " facilityCost " + facilityCost + " t_i " + t_i + " phase_switch size " + phaseSwitch.size());
		
		if(getSuperstep()==0) {
			// vertexADS = LoadADSFromFile(ADSFile, IdToHashMapping);
			//// vertexADS = ((FacilityLocationGiraphWorkerContext) getWorkerContext()).getADS();
			// System.out.println("Superstep 0 in compute of vertex " + getId().get() + " vertexADS size " + vertexADS.size());
			vertexMapping = ((FacilityLocationGiraphWorkerContext) getWorkerContext()).getMapping();
			ADS = getValue().getADS();
			aggregate(MAX_AGG_GAMMA, new DoubleWritable(facilityCost));
		}
		
		if(phase==true) { // run computation to open facilities
			double neighborhoodSize = 0.0;
			// String ADS = vertexADS.get(vertexId);
		
			for(int i=0; i<alpha; i+=distanceStepSize) {
				neighborhoodSize = getNeighborhoodSize(ADS,frozenClients,i,bottom_k); // only consider those nodes that are not frozen in the i-neighborhood.
				t_i += neighborhoodSize * ((((1 + EPS)*alpha) - i) - max(0, (alpha-i)));
				// System.out.println("Ball radius " + i + " Neighborhood size " + neighborhoodSize + " t_i " + t_i);
			}
			
			getValue().setTi(t_i); // save the value of t_i for each vertex
		
			if(t_i >= facilityCost) { // open facility
				getValue().setIsFacilityOpen();
				getValue().setAlphaAtFacilityOpen(alpha);
				aggregate(PHASE, new BooleanWritable(false));
				flag_freeze = 1;
				openFacilities.add(vertexId);
			}
			
			else {
				aggregate(PHASE, new BooleanWritable(true));
			}
			
			aggregate(OPEN_FACILITIES,new MapWritable().getMapWritable(openFacilities));
		}
		else { // run method to send freeze messages
			
			double maxDist = -1000000, maxId = -1;
			
			double remaining_distance = 0, id = 0;
			
			if(openFacilities.contains(vertexId) && flag_freeze==1) { // send initial freeze messages only if its a facility
				flag_freeze = 0;
				// System.out.println("Facility with vertex id " + vertexId);
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(vertexId, (1 + EPS)*alpha);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
			
			else {
				
				int flag = 0;
				
				for (DoublePairWritable message: messages) { //
					id = message.getFirst();
					remaining_distance = message.getSecond();
					
					if(receivedFreezeMessagesFrom.contains(id)==false && id!=vertexId) { // if the vertex already hasnt received a message from this id.. and self loop 
						receivedFreezeMessagesFrom.add(id);
						frozenClients.add(vertexId);
						aggregate(FROZEN_CLIENTS,new MapWritable().getMapWritable(frozenClients));
						// getValue().setReceivedFreezeMessagesFrom(id);
						getValue().setReceivedFreezeMessagesFrom(receivedFreezeMessagesFrom);
						// System.out.println("Received freeze message from " + id + " in vertex " + vertexId + " " + remaining_distance);

						for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
							if(weightedFlag=="0")
								remaining_distance = remaining_distance - 1; // un-weighted case
							else
								remaining_distance = remaining_distance - edge.getValue().get(); // for weighted case
							// System.out.println("edge between " + vertexId + " and " + edge.getTargetVertexId() + " value " + edge.getValue().get() + " remaining " + remaining_distance);
							if(remaining_distance>=0) {
								flag = 1;
								aggregate(PHASE, new BooleanWritable(false));
								DoublePairWritable dpw = new DoublePairWritable(id, remaining_distance);
								sendMessage(edge.getTargetVertexId(), dpw);
							}
						}
					}
				}
				
				if(flag==0) {
					aggregate(PHASE, new BooleanWritable(true));
					boolean pase = this.<BooleanWritable>getAggregatedValue(PHASE).get();
					phaseSwitch.add(vertexId);
					aggregate(PHASE_SWITCH,new MapWritable().getMapWritable(phaseSwitch));
					// System.out.println("Halting id " + vertexId + " phase " + pase);
					// voteToHalt();
				}
			}
		}
	}
}