package org.apache.giraph.examples.parallelMIS;

import java.io.IOException;

import org.apache.giraph.examples.luby.LubysAlgorithmVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ParallelMISOutputFormat extends TextVertexOutputFormat<LongWritable, ParallelMISVertexValue, FloatWritable> {

	@Override
	public TextVertexOutputFormat<LongWritable, ParallelMISVertexValue, FloatWritable>.TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new ParallelMISTextVertexLineWriter();
	}
	
	private class ParallelMISTextVertexLineWriter extends TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(Vertex<LongWritable, ParallelMISVertexValue, FloatWritable, ?> vertex) throws IOException {
			
			// boolean vertexIncluded = vertex.getValue().getVertexIncluded();
			String vertexState = vertex.getValue().getVertexState().toString();
			
			if(vertexState.equals("inS")) {
				StringBuilder sb = new StringBuilder();
				sb.append(vertex.getId());
				// sb.append(":");
				// sb.append(vertex.getValue().getVertexState());
				return new Text(sb.toString());
			}
			else {
				return null;
			}
		}
	}

}
