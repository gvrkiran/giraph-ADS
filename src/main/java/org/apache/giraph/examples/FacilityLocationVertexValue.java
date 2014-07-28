package org.apache.giraph.examples;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class FacilityLocationVertexValue implements Writable {

	private boolean isFrozen;
	private ArrayList<Double> messageReceivedFrom;
	private boolean isFacility;
	private double radius;
	private int facilityCost;
	
	private double falsePositiveProbability = 0.1;
	private int expectedNumberOfElements = 10000;
	private BloomFilter<Double> messageReceivedFromBF;// = new BloomFilter<Double>(falsePositiveProbability, expectedNumberOfElements);
	
	private HyperLogLog counter;

	private ByteArrayList hops = new ByteArrayList();
	private LongArrayList nf = new LongArrayList();
	private double rsd = 0.01;

	public FacilityLocationVertexValue() {
	
	}
	
	public FacilityLocationVertexValue(double radius, int facilityCost) {
		this.isFrozen = false;
		this.isFacility = false;
		this.radius = radius;
		this.facilityCost = facilityCost;
		this.messageReceivedFromBF = new BloomFilter<Double>(falsePositiveProbability, expectedNumberOfElements);
		counter = new HyperLogLog(rsd);
	}
	
	public HyperLogLog counter() {
	    return counter;
	  }

	public void registerEstimate(byte hop, long numReachableVertices) {
		hops.add(hop);
		nf.add(numReachableVertices);
	}
	
	public boolean getIsFrozen() {
		return this.isFrozen;
	}
	
	public boolean getIsFacility() {
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
	
	public double getRadius() {
		return this.radius;
	}

	public int getFacilityCost() {
		return this.facilityCost;
	} 
	
	public FacilityLocationVertexValue findVertexWithId(int id) {
		return null;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(this.isFacility);
	    dataOutput.writeBoolean(this.isFrozen);
	    dataOutput.writeInt(this.facilityCost);
	    dataOutput.writeDouble(this.radius);
	    dataOutput.writeInt(this.expectedNumberOfElements);
	    dataOutput.writeDouble(this.falsePositiveProbability);
	    dataOutput.writeChars(this.messageReceivedFromBF.toString());	   
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.isFacility = dataInput.readBoolean();
		this.isFrozen = dataInput.readBoolean();
		this.facilityCost = dataInput.readInt();
		this.radius = dataInput.readDouble();
		this.expectedNumberOfElements = dataInput.readInt();
		this.falsePositiveProbability = dataInput.readDouble();
		// this.messageReceivedFromBF = dataInput.readChar();
	}
	
}