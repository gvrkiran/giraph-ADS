package org.apache.giraph.examples.parallelMIS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ParallelMISVertexValue implements Writable {

	public double vertexValue = 0.0;
	public String vertexState = "unknown"; // could be one of unknown, tentativelyInS, notInS, inS 
	
	public ParallelMISVertexValue() {
		
	}
	
	public void setVertexValue(double value) {
		this.vertexValue = value;
	}
	
	public double getVertexValue() {
		return this.vertexValue;
	}
	
	public void setVertexState(String state) {
		this.vertexState = state;
	}
	
	public String getVertexState() {
		return this.vertexState;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.vertexValue = dataInput.readDouble();
		this.vertexState = dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeDouble(this.vertexValue);
		dataOutput.writeUTF(this.vertexState);
	}

}
