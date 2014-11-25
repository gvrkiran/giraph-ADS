package org.apache.giraph.examples.facilityAlgorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

public class FacilityLocationGiraphWorkerContext extends WorkerContext {
	
	private static Map<Double, String> vertexADS;
	
	public Map<Double, String> LoadADS(Configuration configuration) {
		
		Path sourceFile = null, sourceFile1 = null;
		try {
			@SuppressWarnings("deprecation")
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
			sourceFile = cacheFiles[0];
			// System.out.println("Source file 1 is " + sourceFile);
	        FileSystem fs = FileSystem.getLocal(configuration);
	        BufferedReader br = new BufferedReader(new InputStreamReader(
	            fs.open(sourceFile)));
	        
	        String line = null;
			String[] line_split = null;
			Map<Double,Double> idToHash = new HashMap<Double,Double>();
			Map<Double,String> vertexADS = new HashMap<Double,String>();
			
			while ((line = br.readLine()) != null) {
				line_split = line.split("\t");
				try {
					idToHash.put(Double.parseDouble(line_split[1]),Double.parseDouble(line_split[0]));				
				} catch(ArrayIndexOutOfBoundsException e) {
					// System.out.println("Something wrong in the input file: " + e);
				}
			}
			
			br.close();
			
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
			return vertexADS;	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Map<Double, String> getADS() {
		return vertexADS;
	}
	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		Configuration configuration = getContext().getConfiguration();
		vertexADS = LoadADS(configuration);
		if(vertexADS.size()==0) {
			// System.out.println("This piece of s**t didnt work");
		}
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
