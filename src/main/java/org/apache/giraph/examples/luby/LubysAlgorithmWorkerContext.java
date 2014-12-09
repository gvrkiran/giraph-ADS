package org.apache.giraph.examples.luby;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

public class LubysAlgorithmWorkerContext extends WorkerContext {
	
	private static Map<Integer, Integer> vertexDegree;
	public static final String NUM_MACHINES = "";
	public static long vertexId = -1;
	
	public Map<Integer, Integer> LoadDegree(Configuration configuration) {
		
		Path sourceFile = null, sourceFile1 = null;
		Map<Integer,Integer> vertexDegree = new HashMap<Integer,Integer>();
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
			
			while ((line = br.readLine()) != null) {
				// System.out.println("Line read " + line);
				line_split = line.split("\t");
				try {
					vertexDegree.put(Integer.parseInt(line_split[0]),Integer.parseInt(line_split[1]));				
				} catch(ArrayIndexOutOfBoundsException e) {
					// System.out.println("Something wrong in the input file: " + e);
				}
			}
			
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vertexDegree;
		// return null;
	}
	
	public Map<Integer, Integer> getDegree() {
		return vertexDegree;
	}
	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		Configuration configuration = getContext().getConfiguration();
		vertexDegree = LoadDegree(configuration);
		if(vertexDegree.size()==0) {
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
