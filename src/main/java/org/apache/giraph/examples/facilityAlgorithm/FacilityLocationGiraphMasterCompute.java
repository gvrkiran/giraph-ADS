package org.apache.giraph.examples.facilityAlgorithm;

import java.io.IOException;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

public class FacilityLocationGiraphMasterCompute extends DefaultMasterCompute {
	
	double alpha = 1.0; // some initial value CHANGE to gamma/(m^2 * (1+eps))
	static double EPS = 0.02; // some value. CHANGE later
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {				
		registerPersistentAggregator(FacilityLocationGiraphVertex.DIST_ALPHA, DoubleSumAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.FROZEN_CLIENTS, FacilityLocationGiraphFreezeAggregator.class);
    }
	
	@Override
	public void compute() {
		// double alpha = getAggregatedValue(FacilityLocationSendFreezeMessages.DIST_ALPHA);
		double alpha = 0;
		alpha = alpha * (1 + EPS);
		setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha));
	}
}
