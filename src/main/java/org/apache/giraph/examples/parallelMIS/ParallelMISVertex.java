package org.apache.giraph.examples.parallelMIS;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.examples.luby.LubysAlgorithmVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

// giraph implementation of the parallel MIS algorithm from http://www.cs.cmu.edu/~jshun/mis.pdf

public class ParallelMISVertex extends Vertex<LongWritable, LubysAlgorithmVertexValue, FloatWritable, DoublePairWritable> {

	public static String PHASE = "computation"; // 2 phases, computation, check_restart;
	public static String REMAINING_UNKNOWN_VERTICES = "";
	public int phase_conflict = -1;
	
	public void conflictResolution(int superStepNum, Iterable<DoublePairWritable> messages) {
		
		double id = 0, value = 0;
		double minValue = getValue().getVertexValue();
		double minId = getId().get();
		
		if(superStepNum==0){ // only send the vertexid to all neighbors
			
			/*
			for (DoublePairWritable message: messages) { // if the vertex receives multiple messages, only propagate the one with the highest remaining distance
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			*/

			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
			
					DoublePairWritable dpw = new DoublePairWritable(minId, minValue);
					// System.out.println("Sending message in phase conflictResolution from " + minId 
						//	+ " to " + edge.getTargetVertexId() + " in superStep " + getSuperstep()
						//	+ " flag " + superStepNum);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
		}
		
		if(superStepNum==1) { // receive messages and check if the minimum is the node itself.
			for (DoublePairWritable message: messages) {
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			
			// send only the minId
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(minId, minValue);
				// System.out.println("Sending message in phase conflictResolution " + " in superStep " + getSuperstep());
				// System.out.println("Sending message in phase conflictResolution from " + minId 
					//	+ " to " + edge.getTargetVertexId() + " in superStep " + getSuperstep()
					//	+ " flag " + superStepNum);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
			
			/*
			if(minId!=getId().get()) { // if you are not the minimum among your neighbors, set your type to notInS.
				getValue().setVertexState("notInS");
			}
			*/
		}
		
		if(superStepNum==2) { // receive from 2 hops and check if the node is the minimum
			for (DoublePairWritable message: messages) {
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			
			if(minId==getId().get()) {
				// System.out.println("Added to S " + minId);
				getValue().setVertexState("inS");
			}
			/*
			else {
				System.out.println("Added to notInS " + minId);
				getValue().setVertexState("notInS");
			}
			*/
		}
	}
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) throws IOException {
		
		long superStepNum = getSuperstep();
		String vertexState = getValue().getVertexState();
		String phase = getAggregatedValue(PHASE).toString();
		
		// System.out.println("Super step: " + superStepNum + " vertex state " + vertexState 
					//		+ " phase " + phase + " entered");
		
		if(vertexState!="unknown") { // only consider those vertices whose state is "unknown"
			voteToHalt();
		}
		
		if(phase.equals("conflict_resolution")) {
			phase_conflict += 1;
			// System.out.println("In conflict resolution, phase_conflict " + phase_conflict);
			// vertexState = getValue().getVertexState();
			conflictResolution(phase_conflict%3, messages);
		}
		
		if(phase.equals("remove_neighbors")) {
			// send message to neighbors only if you are inS
			if(getValue().getVertexState().equals("inS")) {
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) { // send an empty message
					DoublePairWritable dpw = new DoublePairWritable(0,0);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			}
		}
		
		if(phase.equals("check_restart")) { // if there is a vertex with state unknown, it will set the REMAINING_UNKNOWN_VERTICES boolean to false
			phase_conflict=0;
			
			int flag_test = 0;
			for (DoublePairWritable message: messages) { // check to see if a "remove_neighbors" message was received
				flag_test = 1;
			}
			
			if(flag_test==1) { // if a node receives a "remove_neighbors" message, remove it.
				getValue().setVertexState("notInS");
				// System.out.println("here");
			}
			
			vertexState = getValue().getVertexState();
			// System.out.println(vertexState);
			
			if(vertexState.equals("unknown")) {
				// System.out.println("Still unknown id " + getId().get());
				aggregate(REMAINING_UNKNOWN_VERTICES, new BooleanWritable(false));
			}
		}
	}

}