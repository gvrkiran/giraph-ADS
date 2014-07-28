package org.apache.giraph.io;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.FacilityLocationVertexValue;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FacilityLocationTextVertexInputFormat extends TextVertexInputFormat<IntWritable, FacilityLocationVertexValue, IntWritable>{
// public class FacilityLocationTextVertexInputFormat extends TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable>{

	@Override
	  public TextVertexReader createVertexReader(
	          InputSplit split, TaskAttemptContext context) throws IOException {
	    return new FacilityLocationVertexReaderFromEachLine();
	  }

	/**
	* Reads the line and parses them by the following schema:
	* vertexID \t , delimited id's and edge weights separated by :  \t
	* facility cost for this vertex \t radius;
	* 
	* e.g. 123  \t  124:4,125:4,163:3,136:1  \t  23 \t 2
	* for vertex 123, there are 4 neighbors, 124,125,163 and 136 with edge weights 4,4,3 and 1 respectively. The facility cost of vertex 123 is 23 and the desired radius is 2;
	*/
	
	  private class FacilityLocationVertexReaderFromEachLine extends
	          TextVertexReaderFromEachLine {

		@Override
		protected Iterable<Edge<IntWritable, IntWritable>> getEdges(Text line)
		// protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(Text line)
				throws IOException {
		      String[] splitLine = line.toString().split("\t");
		      String[] connectedVertexIds = splitLine[1].split(",");
		      List<Edge<IntWritable, IntWritable>> edges = new ArrayList<Edge<IntWritable, IntWritable>>();
		      // List<Edge<LongWritable, FloatWritable>> edges = new ArrayList<Edge<LongWritable, FloatWritable>>();

		      for (int i = 0; i < connectedVertexIds.length; i++) {
		    	  int targetId = Integer.parseInt(connectedVertexIds[i].split(":")[0]);
		    	  int value = Integer.parseInt(connectedVertexIds[i].split(":")[1]);
		    	  // long targetId = Long.parseLong(connectedVertexIds[i].split(":")[0]);
		    	  // float value = Float.parseFloat(connectedVertexIds[i].split(":")[1]);
		    	  edges.add(EdgeFactory.create(new IntWritable(targetId),new IntWritable(value)));
		    	  // edges.add(EdgeFactory.create(new LongWritable(targetId),new FloatWritable(value)));
		      }

		      return edges;
		}

		@Override
		protected IntWritable getId(Text line) throws IOException {
		// protected LongWritable getId(Text line) throws IOException {
		      String[] splitLine = line.toString().split("\t");
		      int id = Integer.parseInt(splitLine[0]);
		      // long id = Long.parseLong(splitLine[0]);

		      return new IntWritable(id);
		      // return new LongWritable(id);
		}

		@Override
		protected FacilityLocationVertexValue getValue(Text line)
				throws IOException {
				
		      	String[] splitLine = line.toString().split("\t");
		      	int facilityCost = Integer.parseInt(splitLine[2]);
		      	double radius = Double.parseDouble(splitLine[3]);
		      	// double facilityCost = Double.parseDouble(splitLine[2]);

		      	FacilityLocationVertexValue value = new FacilityLocationVertexValue(radius, facilityCost);

		      	return value;
		      	// return new DoubleWritable(facilityCost);
		}
	  }

}
