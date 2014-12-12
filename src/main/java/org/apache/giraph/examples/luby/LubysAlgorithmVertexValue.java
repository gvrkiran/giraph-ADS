package org.apache.giraph.examples.luby;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class LubysAlgorithmVertexValue implements Writable {

	public double vertexValue = 0.0;
	public boolean vertexIncluded = false;
	public boolean vertexIsRemoved = false;
	public int vertexDegree = 0;
	public String vertexState = "unknown"; // could be one of unknown, tentativelyInS, notInS, inS 
	public Set<Double> receivedMessagesFrom = new HashSet<Double>();
	
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
	
	public void setVertexIsRemoved() {
		this.vertexIsRemoved = true;
	}
	
	public boolean getVertexIsRemoved() {
		return this.vertexIsRemoved;
	}
	
	public void setVertexDegree(int degree) {
		this.vertexDegree = degree;
	}
	
	public int getVertexDegree() {
		return this.vertexDegree;
	}
	
	public void setVertexState(String state) {
		this.vertexState = state;
	}
	
	public String getVertexState() {
		return this.vertexState;
	}
	
	public void setReceivedMessagesFrom(double id) {
		receivedMessagesFrom.add(id);
	}
	
	public void setReceivedMessagesFrom(Set<Double> recv) {
		this.receivedMessagesFrom = recv;
	}
	
	public Set<Double> getReceivedMessagesFrom() {
		return receivedMessagesFrom;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.vertexIncluded = dataInput.readBoolean();
		this.vertexValue = dataInput.readDouble();
		this.vertexIsRemoved = dataInput.readBoolean();
		this.vertexDegree = dataInput.readInt();
		this.vertexState = dataInput.readUTF();
		
		int size = dataInput.readInt();
		
		for (int i = 0; i < size; i++) {
		      this.receivedMessagesFrom.add(
		    		  dataInput.readDouble());
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(this.vertexIncluded);
		dataOutput.writeDouble(this.vertexValue);
		dataOutput.writeBoolean(this.vertexIsRemoved);
		dataOutput.writeInt(this.vertexDegree);
		dataOutput.writeUTF(this.vertexState);
		
		dataOutput.writeInt(this.receivedMessagesFrom.size());

		Iterator<Double> iter = this.receivedMessagesFrom.iterator();
		
		while(iter.hasNext()) {
			double tmp = iter.next();
			dataOutput.writeDouble(tmp);
		}
	}

}