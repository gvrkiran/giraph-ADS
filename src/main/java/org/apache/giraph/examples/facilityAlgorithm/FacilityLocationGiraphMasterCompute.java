package org.apache.giraph.examples.facilityAlgorithm;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.metrics.AggregatedMetric;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;

public class FacilityLocationGiraphMasterCompute extends DefaultMasterCompute {
	
	double alpha = 1.0; // some initial value CHANGE to gamma/(m^2 * (1+eps))
	static double EPS = 0.02; // some value. CHANGE later
	double num_vertices = 100; // CHANGE
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		registerPersistentAggregator(FacilityLocationGiraphVertex.PHASE, BooleanAndAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.DIST_ALPHA, DoubleSumAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.FROZEN_CLIENTS, FacilityLocationGiraphFreezeAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.OPEN_FACILITIES, FacilityLocationGiraphFreezeAggregator.class);
    }
	
	@Override
	public void compute() {
		// double alpha = getAggregatedValue(FacilityLocationSendFreezeMessages.DIST_ALPHA);
		
		if(getSuperstep()==0) {
			setAggregatedValue(FacilityLocationGiraphVertex.PHASE, new BooleanWritable(true)); // set phase to true in the first superstep
			setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // set initial value of alpha to ..
		}
		
		alpha = this.<DoubleWritable>getAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA).get();
		alpha = alpha * (1+EPS);
		setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // increment alpha after every superstep -- ?
		
		double num_open_facilities = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.OPEN_FACILITIES).getSize();
		double num_frozen_clients = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.OPEN_FACILITIES).getSize();
		
		double total = num_open_facilities + num_frozen_clients;
		
		if(total>=num_vertices) { // there is NOT at least one non-opened facility and at least one non-frozen
			haltComputation();
		}
		
	}
}
