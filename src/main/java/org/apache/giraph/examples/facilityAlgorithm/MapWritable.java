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
        frozenNodes = new HashMap<Double, Double>();
    }
	
	public MapWritable(double id) {
		frozenNodes = new HashMap<Double, Double>();
		frozenNodes.put(id, 1.0);
	}
	
	public Map<Double, Double> getFrozenNodes() {
		return frozenNodes;
	}
	
	public void setFrozenNodes(Map<Double, Double> map) {
		frozenNodes = map;
	}
	
	public Map<Double, Double> getMapWritable(double id) {
		Map<Double, Double> tmp = new HashMap<Double, Double>();
		tmp.put(id, 1.0);
		return tmp;
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
