package org.apache.giraph.examples.adsLubys;

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
		System.out.println("Superstep num. " + getSuperstep() + " Time taken " + (endTime-startTime)/1000 + " seconds");
		if(getSuperstep()>2)
			haltComputation();
	}

}
