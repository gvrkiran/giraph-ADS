package org.apache.giraph.examples.freeze;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.examples.BloomFilter;
import org.apache.hadoop.io.Writable;

public class FacilityLocationSendFreezeMessagesVertexValue implements Writable {

	private Map<Double, Double> vertexFilter = new HashMap<Double, Double>(); // used temporarily in place of bloom filters. CHANGE after finding a way to serialize bloom filters
	
	double falsePositiveProbability = 0.1;
	int expectedNumberOfElements = 100;
	
	BloomFilter<Double> bloomFilter = new BloomFilter<Double>(falsePositiveProbability, expectedNumberOfElements);
	
	public void setVertexFilter(double id) {
		vertexFilter.put(id,1.0);
		// vertexADS.add(id);
	}
	
	public Map<Double, Double> getVertexFilter() {
		return vertexFilter;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.vertexFilter.put(
		    		  dataInput.readDouble(), dataInput.readDouble());
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.vertexFilter.size());

		for (Entry<Double, Double> entry : this.vertexFilter.entrySet()) {
		      dataOutput.writeDouble(entry.getKey());
		      dataOutput.writeDouble(entry.getValue());
		}
		
		// TO DO -- look for methods for serialization and deserialization of bloom filters
	}
	
}