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

public class Step4 {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// -Second mapper takes kdist of each neighbor to its point and compares to distance  
			//  between neighbor and point, takes max, outputs (p,max of kdist or dist) to reducer
			// -Second reducer averages these to find LRD, outputs {p,LRD}
			String line = value.toString();
			String[] tokens = line.split("\\s+");
			String[] tempPoint;
			String point, neighbor, outputKey, outputValue;
			double kdist = 0, distance = 0, maxDist = 0;
			int px, py, nx, ny;
			Text textKey = new Text();
			Text textValue = new Text();

			// Get point
			point = tokens[0];
			String[] trimmedPoint = point.substring(1, point.length()-1).split(",");
			px = Integer.parseInt(trimmedPoint[0], 10);
			py = Integer.parseInt(trimmedPoint[1], 10);

			// Get Neighbor
			neighbor = tokens[1];
			String[] trimmedNeighbor = neighbor.substring(1, neighbor.length()-1).split(",");
			nx = Integer.parseInt(trimmedNeighbor[0], 10);
			ny = Integer.parseInt(trimmedNeighbor[1], 10);

			// Get kdist
			kdist = Double.parseDouble(tokens[2]);

			// Find distance between neighbor and point, take max of that and kdist
			distance = Math.sqrt(Math.abs(py - ny) * Math.abs(py - ny) + 
					     Math.abs(px - nx) * Math.abs(px - nx));
			maxDist = Math.max(kdist, distance);
			
			// Output key/value pairs, format: (p,reachability distance)
			textKey.set(point);
			textValue.set(Double.toString(maxDist));
			context.write(textKey, textValue);
		}
	}

	// Reducer computes outliers for each segment, corresponding to a square of r to a side
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Reducer receives point and reachability distance, outputs 
			// (p,LRD) for each point

			double dist = 0, sum = 0, count = 0, average, LRD = 0;
			String point = key.toString();
			Text textKey = new Text();
			Text textValue = new Text();

      			for (Text val : values) {
				dist = Double.parseDouble(val.toString());
      				sum += dist;
				count++;
      			}

			average = sum / count;

			if (average > 0) {
				LRD = 1/average;
			}
			
			textKey.set(point);
			textValue.set(Double.toString(LRD));
			context.write(textKey, textValue);
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
			System.err.println("Usage: Step4 (HDFS point file) (HDFS output file)");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Step 4");

		job.setJarByClass(Step4.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
	}
}
