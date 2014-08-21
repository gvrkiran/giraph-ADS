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

public class MapWritable implements Writable {

	private Set<Double> frozenNodes = new HashSet<Double>();
	
	public MapWritable() {
        this.frozenNodes = new HashSet<Double>();
    }
	
	public Set<Double> get() {
		return frozenNodes;
	}
	
	public Set<Double> addElement(double id) {
		this.frozenNodes.add(id);
		return frozenNodes;
	}
	
	public MapWritable(double id) {
		// frozenNodes = new HashMap<Double, Double>();
		this.frozenNodes.add(id);
	}
	
	public MapWritable(Set<Double> set) {
		this.frozenNodes = set;
	}
	
	public Set<Double> getFrozenNodes() {
		return frozenNodes;
	}
	
	public void setFrozenNodes(Set<Double> set) {
		frozenNodes = set;
	}
	
	public MapWritable getMapWritable(Set<Double> set) {
		return new MapWritable(set);
	}
	
	public double getSize() {
		return frozenNodes.size();
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		int size = input.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.frozenNodes.add(input.readDouble());
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.frozenNodes.size());

		Iterator iter = this.frozenNodes.iterator();
		
		while(iter.hasNext()) {
			double tmp = (Double) iter.next();
			output.writeDouble(tmp);
		}
	}

}
