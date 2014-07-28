package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class FacilityLocationVertexValue implements Writable {

	private int isFrozen;
	private ArrayList<Double> messageReceivedFrom;
	private int isFacility;
	private double alpha;
	private int facilityCost;
	
	private double falsePositiveProbability = 0.1;
	private int expectedNumberOfElements = 100;
	private BloomFilter<Double> messageReceivedFromBF = new BloomFilter<Double>(falsePositiveProbability, expectedNumberOfElements);

	public FacilityLocationVertexValue(int k, int facilityCost) {
		isFrozen = 0;
		int i=0;
		for(i=0;i<k;i++) {
			messageReceivedFrom.add(0d);
		}			
		isFacility = 0;
		alpha = 0d;
		this.facilityCost = facilityCost;
	}
	
	public int getIsFrozen() {
		return this.isFrozen;
	}
	
	public int getIsFacility() {
		return this.isFacility;
	}
	
	public ArrayList<Double> getMessageReceivedFrom() {
		return this.messageReceivedFrom;
	}
	
	public BloomFilter<Double> addMessage(Double data) {
		this.messageReceivedFromBF.add(data);
		return messageReceivedFromBF;
	}
	
	public boolean filterContains(Double data) {
		return messageReceivedFromBF.contains(data);
	}
	
	public double getAlpha() {
		return this.alpha;
	}

	public int getFacilityCost() {
		return this.facilityCost;
	}
	
	public FacilityLocationVertexValue findVertexWithId(int id) {
		return null;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}