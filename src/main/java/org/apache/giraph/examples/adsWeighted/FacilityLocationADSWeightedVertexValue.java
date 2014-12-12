package org.apache.giraph.examples.adsWeighted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class FacilityLocationADSWeightedVertexValue implements Writable {

	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	public Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	private Map<Double, Map<Double, Double>> vertexADSTmp = new HashMap<Double, Map<Double, Double>> ();
	private Map<Double, Double> topKHash = new HashMap<Double, Double> ();
	public Map<Double, Double> prevIterAdded = new HashMap<Double, Double> ();
	private double hashValue = 0d;
	private double currentIteration = 0d;
	
	/** Default constructor for reflection */
	public FacilityLocationADSWeightedVertexValue() {
		
	}
	
	public void setHashValue(double hash) {
		this.hashValue = hash;
	}
	
	public double getHashValue() {
		return this.hashValue;
	}
	
	public void setCurrentIteration() {
		this.currentIteration = 0.0;
	}
	
	public void setCurrentIteration(double iteration) {
		this.currentIteration = iteration;
	}
	
	public double getCurrentIteration() {
		return this.currentIteration;
	}
	
	public void setADSTmp(double hash) {
		Map<Double, Double> tmp = new HashMap<Double, Double>();
		tmp.put(hash,0.0);
		this.vertexADSTmp.put(0.0, tmp);
	}
	
	public void setADSTmp(Map<Double, Map<Double, Double>> ADSTmp) {
		this.vertexADSTmp = ADSTmp;
	}
	
	public Map<Double, Map<Double, Double>> getADSTmp() {
		return this.vertexADSTmp;
	}
	
	public void setADS(double id) {
		vertexADS.put(id,0.0);
		// vertexADS.add(id);
	}
	
	public void setADS(Map<Double, Double> ADS) {
		this.vertexADS = ADS;
	}
	
	public Map<Double, Double> getADS() {
		return vertexADS;
	}
	
	public void setPrevIterAdded(double id) {
		prevIterAdded.put(id, 0.0);
	}
	
	public void setPrevIterAdded(Map<Double, Double> prev) {
		this.prevIterAdded = prev;
	}
	
	public Map<Double, Double> getPrevIterAdded() {
		return prevIterAdded;
	}
	
	public Map<Double, Double> setPrevIterAddedEmpty() {
		return new HashMap<Double, Double> ();
	}
	
	/*
	public void setId(double id) {
		this.id = id;
	}
	
	public double getId() {
		return id;
	}
	*/
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		this.hashValue = dataInput.readDouble();
		this.currentIteration = dataInput.readDouble();
		/*
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.vertexADS.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
		*/
		
		int size1 = dataInput.readInt();
		
		for (int i = 0; i < size1; i++) {
		      this.prevIterAdded.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
		
		int size2 = dataInput.readInt();

		for (int i1 = 0; i1 < size2; i1++) {
			int size3 = dataInput.readInt();
			for(int i2 = 0; i2 < size3; i2++) {
				this.topKHash.put(dataInput.readDouble(), dataInput.readDouble());
			}
			this.vertexADSTmp.put(dataInput.readDouble(), this.topKHash);
		}

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		
		dataOutput.writeDouble(this.hashValue);
		dataOutput.writeDouble(this.currentIteration);
		
		/*
		dataOutput.writeInt(this.vertexADS.size());

		for (Entry<Double, Double> entry : this.vertexADS.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		*/
		
		dataOutput.writeInt(this.prevIterAdded.size());

		for (Entry<Double, Double> entry : this.prevIterAdded.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		
		dataOutput.writeInt(this.vertexADSTmp.size());
		
		for (Entry<Double, Map<Double, Double>> entry : this.vertexADSTmp.entrySet()) {
			topKHash = entry.getValue();
			dataOutput.writeInt(topKHash.size());
			Set<Double> keys = topKHash.keySet();
			// Iterator<Double> iterator = topKHash.iterator();
	        for(Double key: keys){
	            dataOutput.writeDouble(key);
	            dataOutput.writeDouble(topKHash.get(key));
	        }
	        
			dataOutput.writeDouble(entry.getKey());
		}
	}
	
}
