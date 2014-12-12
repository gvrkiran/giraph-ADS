package org.apache.giraph.examples.luby;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.ads.DoublePairWritable;
import org.apache.giraph.examples.facilityAlgorithm.FacilityLocationGiraphWorkerContext;
import org.apache.giraph.examples.facilityAlgorithm.MapWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// giraph implementation of Luby's distributed algorithm for computing Maximal independent set
public class LubysAlgorithm extends Vertex<LongWritable, LubysAlgorithmVertexValue, FloatWritable, DoublePairWritable> {

	public int phase_degree = -1;
	public int phase_conflict = -1;
	public int phase_selection = -1;
	public static String PHASE = "degree"; // IntSumAggregator contains mapping from int to phase - 1 = degree, 2 = ..
	public static String REMAINING_UNKNOWN_VERTICES = "";
	Random randomGenerator = new Random();
	// Random randomGenerator = new Random(getId().get()); // not sure if this is correct

	private void degreeComputation(int superStepNum, Iterable<DoublePairWritable> messages) { // argument contains the superstep number relative to degree computation phase
		
		double id = 0;
		int vertexDegree = getValue().getVertexDegree();
		Set<Double> receivedMessagesFrom = getValue().getReceivedMessagesFrom();
		
		if(superStepNum==0) {
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getId().get(), getValue().getVertexValue());
				// System.out.println("Sending message in phase degreeComputation " + " in superStep " + superStepNum);
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		
		else if(superStepNum==1) {
			for (DoublePairWritable message: messages) { // receive and send all messages to the neighbors
				id = message.getFirst();
				if(! receivedMessagesFrom.contains(id)) {
					if(id!=getId().get()) // self loop
						vertexDegree += 1;
				}
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(id, 1);
					// System.out.println("Sending message in phase degreeComputation " + " in superStep " + superStepNum);
					sendMessage(edge.getTargetVertexId(), dpw);
				}
				receivedMessagesFrom.add(id);
			}
			getValue().setVertexDegree(vertexDegree);
			getValue().setReceivedMessagesFrom(receivedMessagesFrom);
		}
		
		else if(superStepNum==2) {
			for (DoublePairWritable message: messages) { // 
				id = message.getFirst();
				if(! receivedMessagesFrom.contains(id)) {
					if(id!=getId().get()) // self loop
						vertexDegree += 1;
				}
				receivedMessagesFrom.add(id);
			}
			getValue().setVertexDegree(vertexDegree);
			getValue().setReceivedMessagesFrom(receivedMessagesFrom);
		}
	}
	
	public void getDegree() {
		Map<Integer, Integer> vertexDegree = new HashMap<Integer, Integer> ();
		vertexDegree = ((LubysAlgorithmWorkerContext) getWorkerContext()).getDegree();
		// System.out.println("Vertex degree size " + vertexDegree.size());
		int vertexId = (int) getId().get();
		int degree = vertexDegree.get(vertexId);
		getValue().setVertexDegree(degree);
	}
	
	public void conflictResolution(int superStepNum, Iterable<DoublePairWritable> messages) {
		
		double id = 0, value = 0;
		/*
		if(superStepNum==0) { // send id, value to neighbors
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				DoublePairWritable dpw = new DoublePairWritable(getId().get(), getValue().getVertexValue());
				sendMessage(edge.getTargetVertexId(), dpw);
			}
		}
		*/
		
		// System.out.println("hereeee! " + superStepNum);
		
		if(superStepNum==0){ // receive messages and send the minimum
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
					/*
					if(minValue<getValue().getVertexValue()) {
						value = minValue;
						id = minId;
					}
					else {
						value = getValue().getVertexValue();
						id = getId().get();
					}
					*/
					DoublePairWritable dpw = new DoublePairWritable(minId, minValue);
					// System.out.println("Sending message in phase conflictResolution " + " in superStep " + getSuperstep());
					sendMessage(edge.getTargetVertexId(), dpw);
				}
			// }
		}
		
		if(superStepNum==1) { // receive messages and check if the minimum is the node itself.
			double minValue = getValue().getVertexValue(), minId = getId().get();
			for (DoublePairWritable message: messages) {
				id = message.getFirst();
				value = message.getSecond();
				if(value<minValue) {
					minValue = value;
					minId = id;
				}
			}
			
			if(/* minValue==getValue().getVertexValue() && */ minId==getId().get()) { // if its the minimum among its 2-hop neighborhood, set type to inS and send message
																				// to neighbors. Otherwise, set type to unknown.
				getValue().setVertexIncluded();
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(minId, minValue);
					// System.out.println("Sending message in phase conflictResolution " + " in superStep " + getSuperstep());
					sendMessage(edge.getTargetVertexId(), dpw);
				}
				getValue().setVertexState("inS");
			}
			else {
				getValue().setVertexState("unknown");
			}
		}
	}
	
	@Override
	public void compute(Iterable<DoublePairWritable> messages) throws IOException {
		
		long superStepNum = getSuperstep();
		String vertexState = "";
		
//		if(phase_conflict>1)
//			phase_conflict = 0;
		
		if(getValue().getVertexState()!="unknown") { // only consider those vertices whose state is "unknown"
			voteToHalt();
		}
		
		String phase = getAggregatedValue(PHASE).toString();
		// boolean restartFlag = this.<BooleanWritable>getAggregatedValue(REMAINING_UNKNOWN_VERTICES).get();
		
		// System.out.println("Super step: " + superStepNum + " phase " + phase + " --verte1x " + getId().get());
		
		if(phase.equals("degree")) {
			// System.out.println("Vertex id " + getId().get());
			phase_degree += 1;
			// degreeComputation(phase_degree, messages);
			getDegree();
		}
		
		else if(phase.equals("selection")) { // Selection step: Takes one superstep. Each vertex v sets its type to TentativelyInS with probability 1/(2×degree(v)),
											// then notifies its neighbors with a message containing its ID.
			// System.out.println("Super step: " + superStepNum + " phase " + phase + " entered");
			int degree = getValue().getVertexDegree();
			// System.out.println("Super step: " + superStepNum + " phase " + phase + " middle1 degree " + degree + " vertex id " + getId().get());
			degree = 2*degree;
			
			if(degree<=0) {
				getValue().setVertexState("notInS");
				voteToHalt();
				// degree = 1; // just for proceeds for this superstep
			}
			
			else {
				int randNum = randomGenerator.nextInt(degree);
				if(randNum==0) { // selected this vertex with probability 1/(2*degree)
					getValue().setVertexState("tentativelyInS");
					// System.out.println("Super step: " + superStepNum + " phase " + phase + " middle2");
					for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
						DoublePairWritable dpw = new DoublePairWritable(getId().get(), getValue().getVertexValue());
						// System.out.println("Sending message in phase selection " + " in superStep " + getSuperstep());
						sendMessage(edge.getTargetVertexId(), dpw);
					}
					// System.out.println("Super step: " + superStepNum + " phase " + phase + " middle3");
				}
				// System.out.println("Super step: " + superStepNum + " phase " + phase + " exit " + randNum);
			}
		}
		
		else if(phase.equals("conflict_resolution")) {
			// System.out.println("Super step: " + superStepNum + " phase " + phase + " entered");
			phase_conflict += 1;
			vertexState = getValue().getVertexState();
			
			if(vertexState.equals("tentativelyInS"))
				conflictResolution(phase_conflict%2, messages);
		}
		
		else if(phase.equals("not_in_s_discovery")) { // if the vertex receives a message in this phase, it becomes inactive and sets its state to notInS
			// System.out.println("Super step: " + superStepNum + " phase " + phase + " entered");
			boolean flag = false;
			for (DoublePairWritable message: messages) { // set flag to true if we receive any message from the neighbors
				flag = true;
			}
			if(flag==true) {
				getValue().setVertexState("notInS");
				for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
					DoublePairWritable dpw = new DoublePairWritable(getId().get(), getValue().getVertexValue());
					// System.out.println("Sending message in phase NotInSDiscovery " + " in superStep " + getSuperstep());
					sendMessage(edge.getTargetVertexId(), dpw);
				}
				voteToHalt();
			}
			
		}
		
		else if(phase.equals("degree_adjusting")) { // every vertex of type "unknown" decreases its degree by the number of messages it receives
			// System.out.println("Super step: " + superStepNum + " phase " + phase + " entered");
			if(getValue().getVertexState().equals("unknown")) {
				int degree = getValue().getVertexDegree();
				for (DoublePairWritable message: messages) {
					degree -= 1;
				}
				getValue().setVertexDegree(degree);
			}
		}
		
		else if(phase.equals("check_restart")) { // if there is a vertex with state unknown, it will set the REMAINING_UNKNOWN_VERTICES boolean to false
			if(getValue().getVertexState().equals("unknown")) {
				// System.out.println("Still unknown id " + getId().get());
				aggregate(REMAINING_UNKNOWN_VERTICES, new BooleanWritable(false));
			}
		}
	}

}