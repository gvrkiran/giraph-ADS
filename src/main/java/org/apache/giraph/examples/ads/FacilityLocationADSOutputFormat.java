package org.apache.giraph.examples.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FacilityLocationADSOutputFormat extends TextVertexOutputFormat<LongWritable, FacilityLocationADSVertexValue, FloatWritable> {

	Random randomGenerator = new Random();
	
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new FacilityLocationADSTextVertexLineWriter();
	}

	  /**
	* Outputs for each line the vertex id and the ADS
	*/
	  private class FacilityLocationADSTextVertexLineWriter extends
	          TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(Vertex<LongWritable, FacilityLocationADSVertexValue, FloatWritable, ?> vertex) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append(vertex.getId());
			sb.append("\t");
			
			/*
			Map<Double, Double> vertexADS = vertex.getValue().getADS();

			for (Entry<Double, Double> entry : vertexADS.entrySet()) {
				sb.append(entry.getKey());
				sb.append(":");
				sb.append(entry.getValue());
				sb.append(";");
			}
			*/

			float randomNum = randomGenerator.nextFloat()*1000; // some random number for Facility weights
			
			sb.append(randomNum);
			sb.append("\t");
			
			// String tmp1 = "";
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				// tmp1 += edge.getTargetVertexId().get() + ":" + edge.getValue() + ";";
				sb.append(edge.getTargetVertexId().get());
				sb.append(":");
				sb.append(edge.getValue());
				sb.append(";");
			}
			
			sb.append("\t");
			
			Map <Double, TreeSet<Double>> vertexADSTmp = vertex.getValue().getADSTmp();
			
			for (Entry<Double, TreeSet<Double>> entry : vertexADSTmp.entrySet()) {
				double distance = entry.getKey();
				TreeSet<Double> tmp = entry.getValue();
				Iterator<Double> iterator = tmp.iterator();
				while(iterator.hasNext()) {
					sb.append(iterator.next());
					sb.append(":");
					sb.append(distance);
					sb.append(";");
				}
				/*
				for(int i=0; i<tmp.size(); i++) {
					sb.append(tmp.get(i));
					sb.append(":");
					sb.append(distance);
					sb.append(";");
				}
				*/
			}
			
			return new Text(sb.toString());
		}
		  
	  }
	
}