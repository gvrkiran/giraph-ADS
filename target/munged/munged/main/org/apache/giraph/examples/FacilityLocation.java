/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
    name = "Facility Location"
)
public class FacilityLocation extends Vertex<LongWritable,
    FacilityLocationVertexValue, IntWritable, IntPairWritable> {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(FacilityLocation.class);
  /** Sum aggregator name */
  private static String SUM_AGG = "sum";
  /** Min aggregator name */
  private static String MIN_AGG = "min";
  /** Max aggregator name */
  private static String MAX_AGG = "max";
  
  /** Define epsilon = 0.02 */
  private static double eps = 0.02;
  
  /** assume that we know how many facilities we want to open */
  private double numFacilities = 100;
  
  private double numClients;
  private Vertex<LongWritable, FacilityLocationVertexValue, IntWritable, IntPairWritable> vertex;
  private FacilityLocationVertexValue vertexValue1 = new FacilityLocationVertexValue(5, 100);
  
  @Override
  public void compute(Iterable<IntPairWritable> messages) {
	  
	  double gamma = getValue().getFacilityCost() + getEdgeValue(getId()).get();
	  double m = numFacilities * (getTotalNumVertices() - numFacilities);
	  double alpha = gamma/(((int)m^2) * (1+eps));
	  
	  int maxDist = -1000000, maxId = 0, remaining_distance;
	  
	  if(getSuperstep()==0) {
		  for (Edge<LongWritable, IntWritable> edge : getEdges()) {
			  remaining_distance = (int) (alpha - edge.getValue().get());
			  if(remaining_distance>0) {
				  sendMessage(edge.getTargetVertexId(), new IntPairWritable(maxId,remaining_distance));
			  }
		  }
	  }
	  else {
		  for (IntPairWritable message: messages) {
			  int id = message.getFirst();
			  remaining_distance = message.getSecond();
			  for (Edge<LongWritable, IntWritable> edge : getEdges()) {
				  remaining_distance = remaining_distance - edge.getValue().get();
				  if(remaining_distance>=0) {
					  // add the node to the bloom filter and increase the counter of the number of neighbors
					  // FacilityLocationVertexValue vertex = findVertexWithId(id);
					  if(id!=edge.getTargetVertexId().get())
						  sendMessage(edge.getTargetVertexId(), new IntPairWritable(maxId,remaining_distance));
				  }
				  else {
					  voteToHalt();
				  }
			  }
		  }
	  }

	  /*
	  for (IntPairWritable message: messages) {
		  int id = message.getFirst();
		  remaining_distance = message.getSecond();
		  // maxDist = Math.max(maxDist, remaining_distance);
		  if(maxDist < remaining_distance) {
			  maxDist = remaining_distance;
			  maxId = id;
		  }
	  }

 	  if(maxDist<=0)
 		  voteToHalt();
	  
 	  for (Edge<LongWritable, IntWritable> edge : getEdges()) {
 		  
 		  if(superStepNum==0) {
 			  remaining_distance = (int) (alpha - edge.getValue().get());
 		  }
 		  else {
 			 remaining_distance = maxDist - edge.getValue().get();
 		  }
 		  
 		  // add all the elements from which we got a message with >=0 distance to the bloom filter.
		  
		  if(remaining_distance>0) {
			  sendMessage(edge.getTargetVertexId(), new IntPairWritable(maxId,remaining_distance));
		  }
	  }	  

	  if(getSuperstep() == 0) {
		  double gamma = getValue().getFacilityCost() + getEdgeValue(getId()).get();
		  double m = numFacilities * (getTotalNumVertices() - numFacilities);
		  double alpha = gamma/(((int)m^2) * (1+eps));
	  }
	  
	  if (getSuperstep() >= 1) {
		  DoubleWritable alpha = new DoubleWritable(getValue().getAlpha());
		  alpha = new DoubleWritable(alpha.get() * (1+eps));
		  
		  processMessages(getSuperstep(), alpha.get(), messages,vertexValue1);
		  		  
		  double sum = 0;
		  ArrayList<Double> frozenNeighbors = new ArrayList<Double>();
		*/
	  /*
		  for (DoubleWritable message : messages) {
			  frozenNeighbors.add(message.get());
			  sum += message.get();
		  }
		  DoubleWritable vertexValue =
				  new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
		  setValue(vertexValue);
		  
		  aggregate(MIN_AGG, new DoubleWritable(vertexValue.get() + 1d));
		  aggregate(MAX_AGG, vertexValue);
		  aggregate(SUM_AGG, new LongWritable(1));
		  LOG.info(getId() + ": PageRank=" + vertexValue +
				  " max=" + getAggregatedValue(MAX_AGG) +
				  " min=" + getAggregatedValue(MIN_AGG));
	  }

	  if(getValue().getIsFacility()!=0 || getValue().getIsFrozen()!=0) { // stop execution on this node if it has been frozen or it has been chosen as a facility.
		  voteToHalt();
	  }
	  
	*/
	  
	  /*
	  if (numFacilities >=1 || numClients >= 1) { // if there is at least one non-opened facility or one non-frozen client
		  long edges = getNumEdges();
		  sendMessageToAllEdges(
				  new DoubleWritable(getValue().get() / edges));
	  } else {
		  voteToHalt();
	  }
	  */
  }

  public void processMessages(long superStepNum, double alpha, Iterable<IntPairWritable> messages, FacilityLocationVertexValue vertexValue) {
	  int maxDist = -1000000, maxId = 0, remaining_distance;
	  
	  for (IntPairWritable message: messages) {
		  int id = message.getFirst();
		  remaining_distance = message.getSecond();
		  // maxDist = Math.max(maxDist, remaining_distance);
		  if(maxDist < remaining_distance) {
			  maxDist = remaining_distance;
			  maxId = id;
		  }
	  }

 	  if(maxDist<=0)
 		  voteToHalt();
	  
 	  for (Edge<LongWritable, IntWritable> edge : getEdges()) {
 		  
 		  if(superStepNum==0) {
 			  remaining_distance = (int) (alpha - edge.getValue().get());
 		  }
 		  else {
 			 remaining_distance = maxDist - edge.getValue().get();
 		  }
 		  
 		  // add all the elements from which we got a message with >=0 distance to the bloom filter.
		  
		  if(remaining_distance>0) {
			  sendMessage(edge.getTargetVertexId(), new IntPairWritable(maxId,remaining_distance));
		  }
	  }
  }
  
  /**
   * Function to return all the nodes in the graph at distance alpha from a particular node.
   * @param alpha
   * @return
   */
  public ArrayList<Double> getNodesAtDistance(double alpha) {
	  ArrayList<Double> nodes = new ArrayList<Double>();
	  return nodes;
  }
  
  /**
   * Worker context used with {@link FacilityLocation}.
   */
  public static class FacilityLocationWorkerContext extends
      WorkerContext {
    /** Final max value for verification for local jobs */
    private static double FINAL_MAX;
    /** Final min value for verification for local jobs */
    private static double FINAL_MIN;
    /** Final sum value for verification for local jobs */
    private static long FINAL_SUM;

    public static double getFinalMax() {
      return FINAL_MAX;
    }

    public static double getFinalMin() {
      return FINAL_MIN;
    }

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
      FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
      FINAL_MAX = this.<DoubleWritable>getAggregatedValue(MAX_AGG).get();
      FINAL_MIN = this.<DoubleWritable>getAggregatedValue(MIN_AGG).get();

      LOG.info("aggregatedNumVertices=" + FINAL_SUM);
      LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
      LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
    }

    @Override
    public void preSuperstep() {
      if (getSuperstep() >= 3) {
        LOG.info("aggregatedNumVertices=" +
            getAggregatedValue(SUM_AGG) +
            " NumVertices=" + getTotalNumVertices());
        if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
            getTotalNumVertices()) {
          throw new RuntimeException("wrong value of SumAggreg: " +
              getAggregatedValue(SUM_AGG) + ", should be: " +
              getTotalNumVertices());
        }
        DoubleWritable maxPagerank = getAggregatedValue(MAX_AGG);
        LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
        DoubleWritable minPagerank = getAggregatedValue(MIN_AGG);
        LOG.info("aggregatedMinPageRank=" + minPagerank.get());
      }
    }

    @Override
    public void postSuperstep() { }
  }

  /**
   * Master compute associated with {@link FacilityLocation}.
   * It registers required aggregators.
   */
  public static class FacilityLocationMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(SUM_AGG, LongSumAggregator.class);
      registerPersistentAggregator(MIN_AGG, DoubleMinAggregator.class);
      registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
    }
  }

  /**
   * Simple VertexReader that supports {@link FacilityLocation}
   */
  public static class FacilityLocationReader extends
      GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(FacilityLocationReader.class);

    @Override
    public boolean nextVertex() {
      return totalRecords > recordsRead;
    }

    @Override
    public Vertex<LongWritable, DoubleWritable,
        FloatWritable, Writable> getCurrentVertex() throws IOException {
      Vertex<LongWritable, DoubleWritable, FloatWritable, Writable>
          vertex = getConf().createVertex();
      LongWritable vertexId = new LongWritable(
          (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
      DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
      long targetVertexId =
          (vertexId.get() + 1) %
          (inputSplit.getNumSplits() * totalRecords);
      float edgeValue = vertexId.get() * 100f;
      List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
      edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
          new FloatWritable(edgeValue)));
      vertex.initialize(vertexId, vertexValue, edges);
      ++recordsRead;
      if (LOG.isInfoEnabled()) {
        LOG.info("next: Return vertexId=" + vertex.getId().get() +
            ", vertexValue=" + vertex.getValue() +
            ", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
      }
      return vertex;
    }
  }

  /**
   * Simple VertexInputFormat that supports {@link FacilityLocation}
   */
  public static class FacilityLocationInputFormat extends
    GeneratedVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
    @Override
    public VertexReader<LongWritable, DoubleWritable,
    FloatWritable> createVertexReader(InputSplit split,
      TaskAttemptContext context)
      throws IOException {
      return new FacilityLocationReader();
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link FacilityLocation}
   */
  public static class FacilityLocationOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new FacilityLocationWriter();
    }

    /**
     * Simple VertexWriter that supports {@link FacilityLocation}
     */
    public class FacilityLocationWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
        throws IOException, InterruptedException {
        getRecordWriter().write(
            new Text(vertex.getId().toString()),
            new Text(vertex.getValue().toString()));
      }
    }
  }
}
