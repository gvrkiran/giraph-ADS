package org.apache.giraph.examples.facilityAlgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class FacilityLocationGiraphVertexValue implements Writable {

	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	public double alphaNode = 0.0;
	public double alphaAtFacilityOpen = 0.0;
	public boolean isFrozen = false;
	public boolean isFacilityOpen = false;
	public double facilityCost = 0;
	public Map<Double, Double> receivedFreezeMessagesFrom = new HashMap<Double, Double>();
	
	/** Default constructor for reflection */
	public FacilityLocationGiraphVertexValue() {
		
	}
	
	public void setFreezeMessagesList(double id) {
		receivedFreezeMessagesFrom.put(id,0.0);
		// vertexADS.add(id);
	}
	
	public Map<Double, Double> getFreezeMessagesList() {
		return receivedFreezeMessagesFrom;
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
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.isFacilityOpen = dataInput.readBoolean();
		this.isFrozen = dataInput.readBoolean();
		this.facilityCost = dataInput.readDouble();
		this.alphaAtFacilityOpen = dataInput.readDouble();
		
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.receivedFreezeMessagesFrom.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.receivedFreezeMessagesFrom.size());

		for (Entry<Double, Double> entry : this.receivedFreezeMessagesFrom.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
	}
	
}
