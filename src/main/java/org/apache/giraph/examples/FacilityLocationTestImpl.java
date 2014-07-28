package org.apache.giraph.examples;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class FacilityLocationTestImpl extends Vertex<LongWritable,
FacilityLocationVertexValue, DoubleWritable, FacilityLocationTestMessage> {
	 /** Number of supersteps for this test */
	  public static final int MAX_SUPERSTEPS = 30;
	  /** Logger */
	  private static final Logger LOG =
	      Logger.getLogger(FacilityLocation.class);
	  /** Sum aggregator name */
	  private static String num_frozen = "sum";
	  /** Min aggregator name */
	  private static String num_facilities = "sum";
	  /** Max aggregator name */
	  private static String MAX_AGG = "max";
	  
	  /** Define epsilon = 0.02 */
	  private static double eps = 0.02;
	  
	  FacilityLocationState state = null;
	  
	  private double numClients;
	  private Vertex<LongWritable, FacilityLocationVertexValue, DoubleWritable, FacilityLocationTestMessage> vertex;
	  // private FacilityLocationVertexValue vertexValue1 = new FacilityLocationVertexValue(5, 100);
	  
	  public enum FacilityLocationState {
		HB, FM;  
	  }
	  
	  @Override
	  public void compute(Iterable<FacilityLocationTestMessage> messages) {
		  
		  // double gamma = getValue().getFacilityCost() + getEdgeValue(getId()).get();
		  // double m = numFacilities * (getTotalNumVertices() - numFacilities);
		  //double alpha = gamma/(((int)m^2) * (1+eps));
		  
		  /*
		  long numFacilities = getAggregatedValue(num_facilities);
		  long numFrozen = getAggregatedValue(num_frozen);
		  
		  if(numFacilities<=0l && numFrozen<=0l) // if there is at least one non-frozen client or one non-opened facility, compute();
			  voteToHalt();
		  
		  
		  if(state==state.HB) {
			  hyperBallComputation(messages);
		  }
		  else if(state==state.FM) {
			  freezeMessageComputation(messages);
		  }
		  */

	  }
	  
	  private int hyperBallComputation(Iterable<FacilityLocationTestMessage> messages) {
		  double numSteps = getValue().getRadius(), remainingDistance = 0;
		  HyperLogLog counter = getValue().counter();

		  if (getSuperstep() == 0L) {
			  counter.offer(getId().get());
			  for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
				  LongWritable vertexId = edge.getTargetVertexId();
				  sendMessage(vertexId, new FacilityLocationTestMessage(vertexId.get(),remainingDistance,counter));
			  }

		  } else {

			  if(getSuperstep()>=numSteps)
				  voteToHalt();
			  
			  long numSeenBefore = counter.cardinality();

			  for (FacilityLocationTestMessage message: messages) {
				  long id = message.getId();
				  remainingDistance = message.getDistance();
				  counter = message.getCounter();
			  // for (HyperLogLog neighborCounter : neighborCounters) {
				  try {
					  counter.merge(counter);
				  } catch (CardinalityMergeException e) {
					  e.printStackTrace();
				  }
			  }

			  long numSeenNow = counter.cardinality();

			  if (numSeenNow > numSeenBefore) {
				  getValue().registerEstimate((byte) getSuperstep(), numSeenNow);
				  for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
					  LongWritable vertexId = edge.getTargetVertexId();
					  // remainingDistance = remainingDistance - getValue().get();
					  sendMessage(vertexId, new FacilityLocationTestMessage(vertexId.get(),remainingDistance,counter));
				  }
			  }
		  }
		  return 0;
	  }
	  
	  private void freezeMessageComputation(Iterable<FacilityLocationTestMessage> messages) {
		  double alpha = 5; // CHANGE -- radius of the ball
		  long maxDist = -1000000, maxId = 0;
		  double remainingDistance, rsd = 0.01;
		  HyperLogLog counter = new HyperLogLog(rsd);
		  
		  if(getSuperstep()==0) {
			  for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
				  remainingDistance = (alpha - edge.getValue().get());
				  if(remainingDistance>0) {
					  // sendMessage(edge.getTargetVertexId(), Iterable<FacilityLocationTestMessage> messages);
				  }
			  }
		  }
		  else {
			  for (FacilityLocationTestMessage message: messages) {
				  long id = message.getId();
				  remainingDistance = message.getDistance();
				  counter = message.getCounter();
				  if(id==getId().get())
					  continue;
				  for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
					  remainingDistance = remainingDistance - edge.getValue().get();
					  if(remainingDistance>=0) {
						  // add the node to the bloom filter and increase the counter of the number of neighbors
						  // FacilityLocationVertexValue vertex = findVertexWithId(id);
						  if(id!=edge.getTargetVertexId().get())
							  sendMessage(edge.getTargetVertexId(), new FacilityLocationTestMessage(maxId,remainingDistance,counter));
					  }
					  else {
						  voteToHalt();
					  }
				  }
			  }
		  }
	  }
	  
	  /**
	   * Master compute associated with {@link FacilityLocationTest}.
	   * It registers required aggregators.
	   */
	  public static class FacilityLocationTestMasterCompute extends
	      DefaultMasterCompute {
	    @Override
	    public void initialize() throws InstantiationException,
	        IllegalAccessException {
	      registerPersistentAggregator(num_facilities, LongSumAggregator.class);
	      registerPersistentAggregator(num_frozen, LongSumAggregator.class);
	      registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
	    }
	  }
}
