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

package org.apache.giraph.examples.hyperball;

// import org.apache.giraph.utils.HyperLogLog;
// import org.apache.giraph.utils.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;

public class HyperBall extends
Vertex<LongWritable, EstimatedNF,
DoubleWritable, HyperLogLog>{

  @Override
  public void compute(
      // Vertex<LongWritable, EstimatedNF, DoubleWritable, HyperLogLog> vertex,
      Iterable<HyperLogLog> neighborCounters) throws IOException {

    HyperLogLog counter = getValue().counter();

    if (getSuperstep() == 0L) {

      counter.offer(getId().get());
      for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
    	  LongWritable vertexId = edge.getTargetVertexId();
    	  sendMessage(vertexId, counter);
      }

    } else {

      long numSeenBefore = counter.cardinality();

      for (HyperLogLog neighborCounter : neighborCounters) {
        try {
			counter.merge(neighborCounter);
		} catch (CardinalityMergeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      }

      long numSeenNow = counter.cardinality();

      if (numSeenNow > numSeenBefore) {
        getValue().registerEstimate((byte) getSuperstep(), numSeenNow);
        for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
      	  LongWritable vertexId = edge.getTargetVertexId();
      	  sendMessage(vertexId, counter);
        }
      }
    }
    voteToHalt();
  }

}