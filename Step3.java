package finalProj.hadoop;

import java.lang.String;
import java.lang.Object;
import java.lang.Math;
import java.awt.Point;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class Step3 {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] tokens = line.split("\\s+");
			String[] tempPoint;
			String outputKey, outputValue;
			double kdist = 0, proposedKdist = 0;
			int px, py, nx, ny;
			Text textKey = new Text();
			Text textValue = new Text();

			// Get point
			String point = tokens[0];
			String[] trimmedPoint = point.substring(1, point.length()-1).split(",");
			px = Integer.parseInt(trimmedPoint[0], 10);
			py = Integer.parseInt(trimmedPoint[1], 10);
			
			// Get neighbors
			for (String neighbor : tokens) {
				tempPoint = neighbor.substring(1, neighbor.length()-1).split(",");
				nx = Integer.parseInt(tempPoint[0], 10);
				ny = Integer.parseInt(tempPoint[1], 10);
				proposedKdist = Math.sqrt(Math.abs(py - ny) * Math.abs(py - ny) + 
							  Math.abs(px - nx) * Math.abs(px - nx));
				// Reset kdist if the distance between this neighbor and the point is greater 
				// than prevous kdist
				kdist = Math.max(kdist,proposedKdist);
			}
			
			// Each point will send a kdist request to the reducer as well as its own kdist  
			// Output key/value pairs, format: {n,(r,p)}, {p,(d,kdist)}
			for (String outPoint : tokens) {
				if (outPoint == point) {
					outputKey = point;
					outputValue = "d," + kdist;
				}
				else {
					outputKey = outPoint;
					outputValue = "r," + point;
				}
				
				textKey.set(outputKey);
				textValue.set(outputValue);
				context.write(textKey, textValue);
			}
		}
	}

	// Reducer computes outliers for each segment, corresponding to a square of r to a side
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Reducer receives kdist and requests for kdist for each point, outputs 
			// {p,(n,kdist)} for each requesting point

			double dist = 0, proposedDist = 0;
			int px, py, nx, ny;
			String neighbor;
			String[] tempNeighbor;
			String point = key.toString();
			Text textKey = new Text();
			Text textValue = new Text();
			List<String> p_list = new ArrayList<String>();
			
			String[] trimmedPoint = point.substring(1, point.length()-1).split(",");

			px = Integer.parseInt(trimmedPoint[0], 10);
			py = Integer.parseInt(trimmedPoint[1], 10);

			for (Text val : values) {
				String value = val.toString();
				// Take point's kdist and set it aside, note that distance sorts first
				// so this will be first in the array
				if (value.charAt(0) == 'd') {
					dist = Double.parseDouble(value.substring(2));
				}
				else {
					p_list.add(value.substring(2));
				}
			}

			// Compute distance between neighbor and point to send to each neighbor
			if (p_list.size() > 0) {
				for (int ir = 0; ir < p_list.size(); ir++) {
					neighbor = p_list.get(ir);
					tempNeighbor = neighbor.substring(1, neighbor.length()-1).split(",");
					nx = Integer.parseInt(tempNeighbor[0], 10);
					ny = Integer.parseInt(tempNeighbor[1], 10);
					proposedDist = Math.sqrt(Math.abs(py - ny) * Math.abs(py - ny) + 
								 Math.abs(px - nx) * Math.abs(px - nx));
					proposedDist = Math.max(dist, proposedDist);

					textKey.set(neighbor);
					textValue.set(point + " " + proposedDist);
					context.write(textKey,textValue);
				}
			}
		}
	}

	private static void setTextOutputFormatSeparator(Configuration conf, String separator) {
		conf.set("mapred.textoutputformat.separator", separator); // Prior to Hadoop 2 (YARN)
		conf.set("mapreduce.textoutputformat.separator", separator); // Hadoop v2+ (YARN)
		conf.set("mapreduce.output.textoutputformat.separator", separator);
		conf.set("mapreduce.output.key.field.separator", separator);
		conf.set("mapred.textoutputformat.separatorText", separator); // ?
	}

	// Run on the Hadoop side
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Step3 (HDFS point file) (HDFS output file)");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Step 5");

		job.setJarByClass(Step3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
	}
}
