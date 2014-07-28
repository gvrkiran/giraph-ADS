package org.apache.giraph.examples;

public class FacilityLocationMessage {
	/**
	* the id of the vertex initiating this message.
	*/
	private long sourceId;
	  
	/**
	* the remaining distance for this message.
	*/
	private long remainingDistance;
	    
	/**
	* Flag, if the remaining distance is greater than 0.
	*/
	private boolean destinationFound = false;
	
	  /**
	* Default constructor for reflection
	*/
	public FacilityLocationMessage() {
	}
	  
	public FacilityLocationMessage(long sourceId, long remainingDistance) {
		this.sourceId = sourceId;
		this.remainingDistance = remainingDistance;
	}
	
}
