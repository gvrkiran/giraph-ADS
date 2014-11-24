package org.apache.giraph.examples.facilityAlgorithm;

import org.apache.giraph.aggregators.DoubleMaxAggregator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.metrics.AggregatedMetric;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;

public class FacilityLocationGiraphMasterCompute extends DefaultMasterCompute {
	
	double gamma = 0.0;
	double alpha = 1.0; // some initial value CHANGE to gamma/(m^2 * (1+eps))
	// static double EPS = 0.2; // some value. CHANGE later
	double num_vertices = 0;
	long startTime = 0, endTime = 0;
	float EPS = 0.2f;
	
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		registerPersistentAggregator(FacilityLocationGiraphVertex.PHASE, BooleanAndAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.DIST_ALPHA, DoubleSumAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.FROZEN_CLIENTS, FacilityLocationGiraphFreezeAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.OPEN_FACILITIES, FacilityLocationGiraphFreezeAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.PHASE_SWITCH, FacilityLocationGiraphFreezeAggregator.class);
		registerPersistentAggregator(FacilityLocationGiraphVertex.MAX_AGG_GAMMA, DoubleMaxAggregator.class);
		startTime = System.currentTimeMillis();
    }
	
	private double computeAlpha() {
		return 0;
	}
	
	@Override
	public void compute() {
		// double alpha = getAggregatedValue(FacilityLocationSendFreezeMessages.DIST_ALPHA);

		endTime = System.currentTimeMillis();
		System.out.println("Superstep num. " + getSuperstep() + " Time taken " + (endTime-startTime)/1000 + " seconds");
		
		if(getSuperstep()==0) {
			EPS = FacilityLocationGiraphVertex.EPSILON.get(getConf());
			System.out.println("EPS " + EPS);
			alpha = computeAlpha();
			setAggregatedValue(FacilityLocationGiraphVertex.PHASE, new BooleanWritable(true)); // set phase to true in the first superstep
			// System.out.println("PHASE " + getAggregatedValue(FacilityLocationGiraphVertex.PHASE));
			setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // set initial value of alpha to ..
			// System.out.println("ALPHA " + getAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA));
		}
		
		else if(getSuperstep()==1) { // in superstep 1, initialize the value of alpha
			// System.out.println("Came here superstep 1");
			gamma = this.<DoubleWritable>getAggregatedValue(FacilityLocationGiraphVertex.MAX_AGG_GAMMA).get();
			num_vertices = getTotalNumVertices();
			double m = num_vertices * num_vertices;
			System.out.println("EPS " + EPS);
			alpha = gamma/(Math.pow(m, 2) *(1+EPS));
			// System.out.println("num vertices " + num_vertices + " gamma " + gamma + " alpha " + alpha);
			setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // increment alpha after every superstep -- ?
		}
		
		else {
						
			// double num_open_facilities = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.OPEN_FACILITIES).getSize();
			// double num_frozen_clients = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.FROZEN_CLIENTS).getSize();
			double num_halted = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.PHASE_SWITCH).getSize();
			
			Set<Double> set1 = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.OPEN_FACILITIES).get();
			Set<Double> set2 = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.FROZEN_CLIENTS).get();
			Set<Double> set3 = new HashSet<Double>(); // set3 contains the unique no. of frozen clients + open facilities
			
			set3.addAll(set1);
			set3.addAll(set2);
			
			double total = set3.size();
			
			if(total<num_vertices && num_halted==num_vertices) { // if not all vertices have been frozen or facilities_opened, and all vertices have halted switch phase.
				// System.out.println("In MASTER PHASE TRUE1");
				setAggregatedValue(FacilityLocationGiraphVertex.PHASE, new BooleanWritable(true));
				setAggregatedValue(FacilityLocationGiraphVertex.PHASE_SWITCH, new MapWritable()); // empty the PHASE_SWITCH map.
				alpha = this.<DoubleWritable>getAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA).get();
				alpha = alpha * (1+EPS);
				setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // increment alpha after every superstep
			}
			
			boolean phase = this.<BooleanWritable>getAggregatedValue(FacilityLocationGiraphVertex.PHASE).get();
			double tmp = this.<MapWritable>getAggregatedValue(FacilityLocationGiraphVertex.PHASE_SWITCH).getSize();
			// System.out.println("Superstep no. " + getSuperstep() + " alpha " + alpha + " phase " + phase + " num halted " + tmp);
			
			if(phase) { // only increment alpha in the facility opening phase
				// System.out.println("In MASTER PHASE TRUE");
				alpha = this.<DoubleWritable>getAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA).get();
				alpha = alpha * (1+EPS);
				setAggregatedValue(FacilityLocationGiraphVertex.DIST_ALPHA, new DoubleWritable(alpha)); // increment alpha after every superstep
			}
					
			phase = this.<BooleanWritable>getAggregatedValue(FacilityLocationGiraphVertex.PHASE).get();

			// System.out.println("HERE total " + total + " Num Vertices " + num_vertices + " phase " + phase);

			if(total>=num_vertices) { // there is NOT at least one non-opened facility and at least one non-frozen
				haltComputation();
			}
		}
	}
}
