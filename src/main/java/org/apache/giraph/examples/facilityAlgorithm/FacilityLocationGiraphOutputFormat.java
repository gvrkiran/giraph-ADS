package org.apache.giraph.examples.facilityAlgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FacilityLocationGiraphOutputFormat extends TextVertexOutputFormat<LongWritable, FacilityLocationGiraphVertexValue, FloatWritable> {

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
		protected Text convertVertexToLine(Vertex<LongWritable, FacilityLocationGiraphVertexValue, FloatWritable, ?> vertex) throws IOException {
			StringBuilder sb = new StringBuilder();
			// sb.append(vertex.getId());
			// sb.append("\t");
			
			// ArrayList<Double> vertexADS = vertex.getValue().getADS();
			Set<Double> vertexReceivedFreezeMessagesFrom = vertex.getValue().getReceivedFreezeMessagesFrom();

			if(vertexReceivedFreezeMessagesFrom.size()!=0) {	
				Iterator<Double> iter = vertexReceivedFreezeMessagesFrom.iterator();
				
				while(iter.hasNext()) {
					double tmp = (Double) iter.next();
					sb.append(vertex.getId());
					sb.append(",");
					sb.append(tmp);
					// sb.append("," + vertex.getValue().getAlphaAtFacilityOpen() + "\n");
					sb.append(",1\n");
				}
				return new Text(sb.toString().trim());
			}
			return null;
		}
	  }
	  
}
