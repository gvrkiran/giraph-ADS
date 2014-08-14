package org.apache.giraph.examples.facilityAlgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class MapWritable implements Writable {

	private Map<Double, Double> frozenNodes = new HashMap<Double, Double>();
	
	public MapWritable() {
        this.frozenNodes = new HashMap<Double, Double>();
    }
	
	public Map<Double, Double> get() {
		return frozenNodes;
	}
	
	public Map<Double, Double> addElement(double id) {
		this.frozenNodes.put(id, 1.0);
		return frozenNodes;
	}
	
	public MapWritable(double id) {
		// frozenNodes = new HashMap<Double, Double>();
		this.frozenNodes.put(id, 1.0);
	}
	
	public MapWritable(Map<Double, Double> map) {
		this.frozenNodes = map;
	}
	
	public Map<Double, Double> getFrozenNodes() {
		return frozenNodes;
	}
	
	public void setFrozenNodes(Map<Double, Double> map) {
		frozenNodes = map;
	}
	
	public MapWritable getMapWritable(Map<Double, Double> map) {
		return new MapWritable(map);
	}
	
	public double getSize() {
		return frozenNodes.size();
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		int size = input.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.frozenNodes.put(
		    		  input.readDouble(), input.readDouble());
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.frozenNodes.size());

		for (Entry<Double, Double> entry : this.frozenNodes.entrySet()) {
		      output.writeDouble(entry.getKey());
		      output.writeDouble(entry.getValue());
		}
	}

}
