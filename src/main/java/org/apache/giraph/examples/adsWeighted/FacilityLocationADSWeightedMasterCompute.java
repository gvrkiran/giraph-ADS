package org.apache.giraph.examples.adsWeighted;

import org.apache.giraph.examples.ads.FacilityLocationADS;
import org.apache.giraph.master.DefaultMasterCompute;

public class FacilityLocationADSWeightedMasterCompute extends DefaultMasterCompute{
	
	long startTime = 0, endTime = 0;
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public void compute() {
		endTime = System.currentTimeMillis();
		int maxIterations = FacilityLocationADSWeighted.MAX_ITERATIONS.get(getConf());
		System.out.println("Superstep num. " + getSuperstep() + " maxIterations " + maxIterations);
		if(getSuperstep()>maxIterations)
			haltComputation();
	}

}
