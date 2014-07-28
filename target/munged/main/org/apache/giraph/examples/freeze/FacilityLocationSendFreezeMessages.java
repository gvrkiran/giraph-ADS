package org.apache.giraph.examples.freeze;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.examples.FacilityLocationVertexValue;
import org.apache.giraph.examples.IntPairWritable;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.examples.ads.FacilityLocationADSVertexValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.graph.Vertex;

/**
 * Demonstrates the basic Pregel implementation for sending freeze messages to a ball of radius R.
 */
@Algorithm(
		name = "Facility Location send freeze messages",
		description = "Facility location routine for sending freeze messages to a ball of radius R."
		)

public class FacilityLocationSendFreezeMessages extends Vertex<DoubleWritable, FacilityLocationSendFreezeMessagesVertexValue, FloatWritable, DoublePairWritable> {

	public static String DIST_ALPHA = "distanceAlpha";
	@Override
	public void compute(Iterable<DoublePairWritable> messages) throws IOException {
		long superStepNum = getSuperstep();
		// double alpha = getAggregatedValue(DIST_ALPHA), maxDist = -1000000, maxId = -1;
		double alpha = 0, maxDist = -1000000, maxId = -1;
		
		double remaining_distance = 0, id = 0;
		Map<Double, Double> vertexFilter = new HashMap<Double, Double>(); // add vertices which have already been seen

		if(superStepNum==0) {
			for (Edge<DoubleWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getId().get(), alpha);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		for (DoublePairWritable message: messages) { // if the vertex receives multiple messages, only propagate the one with the highest remaining distance
			id = message.getFirst();
			remaining_distance = message.getSecond();
			if(maxDist < remaining_distance) {
				maxDist = remaining_distance;
				maxId = id;
			}
		}
		
		if(vertexFilter.containsKey(maxId)==false && id!=getId().get()) { // if the vertex already hasnt received a message from this id.. and self loop 
			vertexFilter.put(id, remaining_distance);
			remaining_distance = maxDist - 1;
			if(remaining_distance>=0) {
				for (Edge<DoubleWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(maxId, remaining_distance);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
			else {
				voteToHalt();
			}
		}
	}
}