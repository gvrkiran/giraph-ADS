package org.apache.giraph.examples.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FacilityLocationADSOutputFormat extends TextVertexOutputFormat<LongWritable, FacilityLocationADSVertexValue, FloatWritable> {

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

			Map <Double, ArrayList<Double>> vertexADSTmp = vertex.getValue().getADSTmp();
			
			for (Entry<Double, ArrayList<Double>> entry : vertexADSTmp.entrySet()) {
				double distance = entry.getKey();
				ArrayList<Double> tmp = entry.getValue();
				for(int i=0; i<tmp.size(); i++) {
					sb.append(tmp.get(i));
					sb.append(":");
					sb.append(distance);
					sb.append(";");
				}
			}
			
			return new Text(sb.toString());
		}
		  
	  }
	
}