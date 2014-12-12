package org.apache.giraph.examples.facilityAlgorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FacilityLocationGiraphWorkerContext extends WorkerContext {
	
	private static Map<Double, Double> vertexMapping;
	public static final String NUM_MACHINES = "";
	public static long vertexId = -1;
	public static int myNum = -1;
	
	public Map<Double, Double> LoadMapping(Configuration configuration) {
		
		Path sourceFile = null, sourceFile1 = null, sourceFile2 = null;
		try {
			@SuppressWarnings("deprecation")
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
			sourceFile = cacheFiles[0];
			// sourceFile2 = cacheFiles[2];
			
			// String[] tmpp = sourceFile2.toString().split("/");
			// double numMachines = Double.parseDouble(tmpp[tmpp.length-1]);
			// System.out.println("NUM MACHINES " + numMachines);
			
			// System.out.println("Source file 1 is " + sourceFile);
	        FileSystem fs = FileSystem.getLocal(configuration);
	        BufferedReader br = new BufferedReader(new InputStreamReader(
	            fs.open(sourceFile)));
	        
	        String line = null;
			String[] line_split = null;
			Map<Double,Double> idToHash = new HashMap<Double,Double>();
			
			while ((line = br.readLine()) != null) {
				line_split = line.split("\t");
				try {
					idToHash.put(Double.parseDouble(line_split[1]),Double.parseDouble(line_split[0]));				
				} catch(ArrayIndexOutOfBoundsException e) {
					// System.out.println("Something wrong in the input file: " + e);
				}
			}
			
			br.close();
			
			/*
			sourceFile1 = cacheFiles[1];
			// System.out.println("Source file 2 is " + sourceFile1);
			FileSystem fs1 = FileSystem.getLocal(configuration);
	        BufferedReader br1 = new BufferedReader(new InputStreamReader(
	            fs1.open(sourceFile1)));
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
			*/
			return idToHash;	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Map<Double, Double> getMapping() {
		return vertexMapping;
	}
	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		Configuration configuration = getContext().getConfiguration();
		// int numMachines = configuration.getInt(NUM_MACHINES,1);
		
		vertexMapping = LoadMapping(configuration);
		// if(vertexADS.size()==0) {
			// System.out.println("This piece of s**t didnt work");
		// }
	}

	public int getMyWorkerIndex() {
		return myNum;
	}
	
	@Override
	public void preSuperstep() {
	}
	
	@Override
	public void postApplication() {	
	}

	@Override
	public void postSuperstep() {	
	}

}
