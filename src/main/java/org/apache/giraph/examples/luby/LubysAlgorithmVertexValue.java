package org.apache.giraph.examples.luby;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LubysAlgorithmVertexValue implements Writable {

	public double vertexValue = 0.0;
	public boolean vertexIncluded = false;
	
	/** Default constructor for reflection */
	public LubysAlgorithmVertexValue() {
		
	}
	
	public void setVertexValue(double value) {
		this.vertexValue = value;
	}
	
	public double getVertexValue() {
		return this.vertexValue;
	}
	
	public void setVertexIncluded() {
		this.vertexIncluded = true;
	}
	
	public boolean getVertexIncluded() {
		return this.vertexIncluded;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.vertexIncluded = dataInput.readBoolean();
		this.vertexValue = dataInput.readDouble();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(this.vertexIncluded);
		dataOutput.writeDouble(this.vertexValue);
	}

}