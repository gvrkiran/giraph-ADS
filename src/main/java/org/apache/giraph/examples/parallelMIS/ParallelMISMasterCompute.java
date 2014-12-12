package org.apache.giraph.examples.parallelMIS;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.examples.luby.LubysAlgorithm;
import org.apache.giraph.examples.luby.TextOverwriteAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class ParallelMISMasterCompute extends DefaultMasterCompute {

	int step_num = -1;
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerPersistentAggregator(ParallelMISVertex.PHASE, TextOverwriteAggregator.class);
		registerPersistentAggregator(ParallelMISVertex.REMAINING_UNKNOWN_VERTICES, BooleanAndAggregator.class);
		// startTime = System.currentTimeMillis();
	}
	
	@Override
	public void compute() {
		
		String phase = getAggregatedValue(ParallelMISVertex.PHASE).toString();
		
		// System.out.println("Phase " + phase + " step_num " + step_num);
		
		if(getSuperstep()==0) {
			setAggregatedValue(ParallelMISVertex.PHASE, new Text("conflict_resolution")); // set phase to 1 in the first superstep
			setAggregatedValue(ParallelMISVertex.REMAINING_UNKNOWN_VERTICES, new BooleanWritable(true));
			step_num += 1;
		}

		if(step_num==0 || step_num==1 || step_num==2) {
			setAggregatedValue(ParallelMISVertex.PHASE, new Text("conflict_resolution"));
			step_num += 1;
		}
		
		else if(step_num==3) {
			setAggregatedValue(ParallelMISVertex.PHASE, new Text("remove_neighbors"));
			step_num += 1;
		}
		
		else if(step_num==4) {
			setAggregatedValue(ParallelMISVertex.PHASE, new Text("check_restart"));
			step_num += 1;
		}
		
		else if(step_num>4) {
			boolean restartFlag = this.<BooleanWritable>getAggregatedValue(ParallelMISVertex.REMAINING_UNKNOWN_VERTICES).get();
			// System.out.println("restartFlag " + restartFlag);
			if(restartFlag==false) { // restart from selection step again
				setAggregatedValue(ParallelMISVertex.PHASE, new Text("conflict_resolution"));
				setAggregatedValue(ParallelMISVertex.REMAINING_UNKNOWN_VERTICES, new BooleanWritable(true));
				step_num = 1;
			}
			else { // halt computation
				haltComputation();
			}
		}
		
	}
}
