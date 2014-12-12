package org.apache.giraph.examples.luby;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LubysAlgorithmOutputFormat extends TextVertexOutputFormat<LongWritable, LubysAlgorithmVertexValue, FloatWritable> {

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new LubysAlgorithmTextVertexLineWriter();
	}

	
	private class LubysAlgorithmTextVertexLineWriter extends TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(Vertex<LongWritable, LubysAlgorithmVertexValue, FloatWritable, ?> vertex) throws IOException {
			
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