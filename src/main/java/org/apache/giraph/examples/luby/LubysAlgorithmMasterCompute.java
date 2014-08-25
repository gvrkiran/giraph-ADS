package org.apache.giraph.examples.luby;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphVertex;
import org.apache.giraph.examples.facilityAlgorithm.MapWritable;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class LubysAlgorithmMasterCompute extends DefaultMasterCompute {
		
	int step_num = -1;
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerPersistentAggregator(LubysAlgorithm.PHASE, TextOverwriteAggregator.class);
		registerPersistentAggregator(LubysAlgorithm.REMAINING_UNKNOWN_VERTICES, BooleanAndAggregator.class);
	}
	
	@Override
	public void compute() {
		
		String phase = getAggregatedValue(LubysAlgorithm.PHASE).toString();
		
		if(getSuperstep()==0) {
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("degree")); // set phase to 1 in the first superstep
			setAggregatedValue(LubysAlgorithm.REMAINING_UNKNOWN_VERTICES, new BooleanWritable(true));
		}
		
		else if(getSuperstep()<=2 && phase.equals("degree"))
			step_num = 0;
		
		else if(getSuperstep()>2 && step_num==0) {
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("selection"));
			step_num = 1;
		}
		
		else if(step_num>=1 && step_num<3) {
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("conflict_resolution"));
			step_num += 1;
		}
		
		else if(step_num==3) {
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("not_in_s_discovery"));
			step_num += 1;
		}
		
		else if(step_num==4) {
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("degree_adjusting"));
			step_num += 1;
		}
		
		else if(step_num==5) { // check if we have to restart
			setAggregatedValue(LubysAlgorithm.PHASE, new Text("check_restart"));
			step_num += 1;
		}
		
		else if(step_num==6) {
			boolean restartFlag = this.<BooleanWritable>getAggregatedValue(LubysAlgorithm.REMAINING_UNKNOWN_VERTICES).get();
			if(restartFlag==false) { // restart from selection step again
				setAggregatedValue(LubysAlgorithm.PHASE, new Text(""));
				setAggregatedValue(LubysAlgorithm.REMAINING_UNKNOWN_VERTICES, new BooleanWritable(true));
				step_num = 0;
			}
			else { // halt computation
				haltComputation();
			}
		}
	}
	
}
