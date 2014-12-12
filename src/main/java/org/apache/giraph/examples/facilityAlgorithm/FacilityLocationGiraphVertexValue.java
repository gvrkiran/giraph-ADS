package org.apache.giraph.examples.facilityAlgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class FacilityLocationGiraphVertexValue implements Writable {

	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	public double alphaNode = 0.0;
	public double alphaAtFacilityOpen = 0.0;
	public boolean isFrozen = false;
	public boolean isFacilityOpen = false;
	public double facilityCost = 0;
	public double t_i = 0;
	public Set<Double> receivedFreezeMessagesFrom = new HashSet<Double>();
	public String ADS = null;
	
	/** Default constructor for reflection */
	public FacilityLocationGiraphVertexValue() {
		
	}
	
	public void setReceivedFreezeMessagesFrom(double id) {
		this.receivedFreezeMessagesFrom.add(id);
		// vertexADS.add(id);
	}
	
	public void setReceivedFreezeMessagesFrom(Set<Double> received) {
		this.receivedFreezeMessagesFrom = received;
	}
	
	public Set<Double> getReceivedFreezeMessagesFrom() {
		return this.receivedFreezeMessagesFrom;
	}
	
	public void setADS() {
		this.ADS = "";
	}
	
	public void setADS(String ads) {
		this.ADS = ads;
	}
	
	public String getADS() {
		return this.ADS;
	}
	
	public void setFacilityCost(double facilityCost) {
		this.facilityCost = facilityCost;
	}
	
	public double getFacilityCost() {
		return facilityCost;
	}
	
	public void setIsFacilityOpen() {
		isFacilityOpen = true;
	}
	
	public boolean getIsFacilityOpen() {
		return isFacilityOpen;
	}
	
	public void setIsFrozen() {
		isFrozen = true;
	}
	
	public boolean getIsFrozen() {
		return isFrozen;
	}
	
	public void setAlphaAtFacilityOpen(double alpha) {
		this.alphaAtFacilityOpen = alpha;
	}
	
	public double getAlphaAtFacilityOpen() {
		return alphaAtFacilityOpen;
	}
	
	public void setTi(double t_i) {
		this.t_i = t_i;
	}
	
	public double getTi() {
		return this.t_i;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.isFacilityOpen = dataInput.readBoolean();
		this.isFrozen = dataInput.readBoolean();
		this.facilityCost = dataInput.readDouble();
		this.alphaAtFacilityOpen = dataInput.readDouble();
		
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.receivedFreezeMessagesFrom.add(
		    		  dataInput.readDouble());
		}
		
		this.ADS = dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(isFacilityOpen);
		dataOutput.writeBoolean(isFrozen);
		dataOutput.writeDouble(facilityCost);
		dataOutput.writeDouble(alphaAtFacilityOpen);
		
		dataOutput.writeInt(this.receivedFreezeMessagesFrom.size());

		Iterator<Double> iter = this.receivedFreezeMessagesFrom.iterator();
		
		while(iter.hasNext()) {
			double tmp = iter.next();
			dataOutput.writeDouble(tmp);
		}
		
		dataOutput.writeUTF(ADS);
		/*
		dataOutput.writeInt(this.ADS.size());
		
		for (Entry<Double, Double> entry : this.ADS.entrySet()) {
			dataOutput.writeDouble(entry.getKey());
			dataOutput.writeDouble(entry.getValue());
		}
		*/
	}
	
}