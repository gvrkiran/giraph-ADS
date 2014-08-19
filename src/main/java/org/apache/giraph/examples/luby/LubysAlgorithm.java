package org.apache.giraph.examples.luby;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

// giraph implementation of Luby's distributed algorithm for computing Maximal independent set
public class LubysAlgorithm extends Vertex<LongWritable, LubysAlgorithmVertexValue, FloatWritable, DoublePairWritable> {

	public static String DIST_ALPHA = "distanceAlpha";
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) throws IOException {
		
		long superStepNum = getSuperstep();
		double id = 0, value = 0;
		
		if(superStepNum==0) { // send id, value to neighbors
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getId().get(), getValue().getVertexValue());
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		if(superStepNum==1){ // receive messages and send the minimum
			double minValue = getValue().getVertexValue(), minId = getId().get();
			
			for (DoublePairWritable message: messages) { // if the vertex receives multiple messages, only propagate the one with the highest remaining distance
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			// if(minValue!=getValue().get()) { // if one of the neighbors has a minimum value less than the value of this node, send it to the neighbors
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(minId, minValue);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			// }
		}
		
		if(superStepNum==2) { // receive messages and check if the minimum is the node itself.
			double minValue = getValue().getVertexValue(), minId = getId().get();
			for (DoublePairWritable message: messages) { // if the vertex receives multiple messages, only propagate the one with the highest remaining distance
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			
			if(minValue==getValue().getVertexValue() && minId==getId().get()) {
				getValue().setVertexIncluded();
			}
		}
		
		if(superStepNum>2) { // only run for 2 supersteps
			voteToHalt();
		}
		
	}

}