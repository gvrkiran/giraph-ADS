package org.apache.giraph.io;

import org.apache.giraph.examples.FacilityLocationVertexValue;
import org.apache.giraph.examples.IntPairWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

public class FacilityLocationTextVertexOutputFormat extends
TextVertexOutputFormat<IntWritable, FacilityLocationVertexValue, IntWritable> {

	@Override
	public TextVertexWriter createVertexWriter( TaskAttemptContext context) throws IOException, InterruptedException {
		return new FacilityLocationTextVertexLineWriter();
	}

	
	private class FacilityLocationTextVertexLineWriter extends
	TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, FacilityLocationVertexValue, IntWritable, ?> arg0)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}
/*
		@Override
		protected Text convertVertexToLine( Vertex<IntWritable, FacilityLocationVertexValue,
				IntWritable, ?> vertex) throws IOException {
			StringBuilder sb = new StringBuilder();

			sb.append(vertex.getId());
			sb.append(" \t");

			for (Map.Entry<Long, Integer> entry : vertex.getValue()
					.getVerticesWithHopsCount().entrySet()) {
				sb.append(entry.getKey());
				sb.append("(");
				sb.append(entry.getValue());
				sb.append(") ");
			}

			return new Text(sb.toString());
		}
		*/
	}
}