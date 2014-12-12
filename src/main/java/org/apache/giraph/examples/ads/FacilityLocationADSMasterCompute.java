package org.apache.giraph.examples.ads;

import org.apache.giraph.master.DefaultMasterCompute;

public class FacilityLocationADSMasterCompute extends DefaultMasterCompute{
	
	long startTime = 0, endTime = 0;
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public void compute() {
		endTime = System.currentTimeMillis();
		int maxDistance = FacilityLocationADS.MAX_DISTANCE.get(getConf());
		System.out.println("Superstep num. " + getSuperstep() + " maxDistance " + maxDistance);
		if(getSuperstep()>maxDistance)
			haltComputation();
	}

}
