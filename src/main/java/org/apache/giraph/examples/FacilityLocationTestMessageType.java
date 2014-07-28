package org.apache.giraph.examples;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class FacilityLocationTestMessageType {
	private long id;
	private double distance;
	private HyperLogLog counter;
	
	  public FacilityLocationTestMessageType(long id, double distance, HyperLogLog counter) {
		  id = id;
		  distance = distance;
		  counter = counter;
	  }
	  
	  public long getId() {
		  return id;
	  }
	  
	  public void setId(long id) {
		  this.id = id;
	  }
	  
	  public double getDistance() {
		  return distance;
	  }
	  
	  public void setDistance(double distance) {
		  this.distance = distance;
	  }
	  
	  public HyperLogLog getCounter() {
		  return counter;
	  }
	  
	  public void setCounter(long id) {
		  this.counter.offer(id);
	  }
	  
}
