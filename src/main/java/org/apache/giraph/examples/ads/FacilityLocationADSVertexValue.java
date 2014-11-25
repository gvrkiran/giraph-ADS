package org.apache.giraph.examples.ads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class FacilityLocationADSVertexValue implements Writable {

	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	// private Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	private Map<Double, ArrayList<Double>> vertexADSTmp = new HashMap<Double, ArrayList<Double>> ();
	private ArrayList<Double> topKHash = new ArrayList<Double> ();
	private double hashValue = 0d;
	private double currentDistance = 0d;
	
	/*
	public void setADS(double hash) {
		vertexADS.put(hash,0.0);
		// vertexADS.add(id);
	}
	
	public Map<Double, Double> getADS() {
		return vertexADS;
	}
	*/
	
	public void setHashValue(double hash) {
		this.hashValue = hash;
	}
	
	public double getHashValue() {
		return this.hashValue;
	}
	
	public void setCurrentDistance() {
		this.currentDistance = 0.0;
	}
	
	public void setCurrentDistance(double distance) {
		this.currentDistance = distance;
	}
	
	public double getCurrentDistance() {
		return this.currentDistance;
	}
	
	public void setADSTmp(double hash) {
		ArrayList<Double> tmp = new ArrayList();
		tmp.add(hash);
		this.vertexADSTmp.put(0.0, tmp);
	}
	
	public void setADSTmp(Map<Double, ArrayList<Double>> ADSTmp) {
		this.vertexADSTmp = ADSTmp;
	}
	
	public Map<Double, ArrayList<Double>> getADSTmp() {
		return this.vertexADSTmp;
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

		hashValue = dataInput.readDouble();
		currentDistance = dataInput.readDouble();
		/*
		int size = dataInput.readInt();

		for (int i = 0; i < size; i++) {
			this.vertexADS.put(dataInput.readDouble(), dataInput.readDouble());
		}
		*/

		int size1 = dataInput.readInt();

		for (int i1 = 0; i1 < size1; i1++) {
			int size2 = dataInput.readInt();
			for(int i2 = 0; i2 < size2; i2++) {
				this.topKHash.add(dataInput.readDouble());
			}
			this.vertexADSTmp.put(dataInput.readDouble(), this.topKHash);
		}
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeDouble(this.hashValue);
		dataOutput.writeDouble(this.currentDistance);
		/*
		dataOutput.writeInt(this.vertexADS.size());

		for (Entry<Double, Double> entry : this.vertexADS.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		*/
		
		dataOutput.writeInt(this.vertexADSTmp.size());
		
		for (Entry<Double, ArrayList<Double>> entry : this.vertexADSTmp.entrySet()) {
			topKHash = entry.getValue();
			dataOutput.write(topKHash.size());
			for (int i = 0; i < topKHash.size(); i++) {
				dataOutput.writeDouble(topKHash.get(i));
			}
			dataOutput.writeDouble(entry.getKey());
		}
	}
	
}