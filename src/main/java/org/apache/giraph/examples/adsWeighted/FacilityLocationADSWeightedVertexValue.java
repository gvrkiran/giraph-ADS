package org.apache.giraph.examples.adsWeighted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class FacilityLocationADSWeightedVertexValue implements Writable {

	// private ArrayList<Double> vertexADS = new ArrayList<Double>();
	public Map<Double, Double> vertexADS = new HashMap<Double, Double>();
	public Map<Double, Double> prevIterAdded = new HashMap<Double, Double> ();
	
	/** Default constructor for reflection */
	public FacilityLocationADSWeightedVertexValue() {
		
	}
	
	public void setADS(double id) {
		vertexADS.put(id,0.0);
		// vertexADS.add(id);
	}
	
	public Map<Double, Double> getADS() {
		return vertexADS;
	}
	
	public void setPrevIterAdded(double id) {
		prevIterAdded.put(id, 0.0);
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
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.vertexADS.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
		
		int size1 = dataInput.readInt();
		
		for (int i = 0; i < size1; i++) {
		      this.prevIterAdded.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
		/*
		for (int i = 0; i < size; i++) {
			this.vertexADS.add(dataInput.readDouble());
		}
		*/
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.vertexADS.size());

		for (Entry<Double, Double> entry : this.vertexADS.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		
		dataOutput.writeInt(this.prevIterAdded.size());

		for (Entry<Double, Double> entry : this.prevIterAdded.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		
		/*
		Iterator<Double> iterator = vertexADS.iterator();
		while (iterator.hasNext()) {
		    dataOutput.writeDouble(iterator.next());
		  }
		  */
	}
	
}
