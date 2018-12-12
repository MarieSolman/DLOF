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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Step5 {

	public static class NeighborMapper extends Mapper<Object, Text, Text, Text> {
		@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Mapper 1: Import points/neighbors, map neighbor and requesting point 

			String line = value.toString();
			String[] tokens = line.split("\\s+");
			String outputKey, outputValue;
			Text textKey = new Text();
			Text textValue = new Text();

			// Get point
			String point = tokens[0];
			
			// Get neighbors
			for (String neighbor : tokens) {
				if (!neighbor.equals(point)) {
					textKey.set(neighbor);
					textValue.set("r," + point);
					// Request each neighbor per point
					context.write(textKey, textValue);
				}
			}
			
		}
	}

	public static class LRDMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Mapper 1: Import points/LRDs, map point and LRD to reducer

			String line = value.toString();
			String[] tokens = line.split("\\s+");
			double LRD;
			String point;
			String outputKey, outputValue;
			Text textKey = new Text();
			Text textValue = new Text();

			// Get point
			point = tokens[0];

			// Get LRD
			LRD = Double.parseDouble(tokens[1]);
			
			// send point and LRD to reducer
			textKey.set(point);
			textValue.set("l," + Double.toString(LRD));
			context.write(textKey, textValue);
		}
	}

	// Reducer computes outliers for each segment, corresponding to a square of r to a side
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Reducer 1: Receive per neighbor its own LRD and list of points requesting LRD, 
			// output point with LRD of neighbor

			double LRD = 0;
			String point;
			String[] tempNeighbor;
			String neighbor = key.toString();
			Text textKey = new Text();
			Text textValue = new Text();
			List<String> p_list = new ArrayList<String>();

			for (Text val : values) {
				String value = val.toString();

				// Take point's LRD and set it aside, note that LRD sorts first
				// so this will be first in the array
				if (value.charAt(0) == 'l') {
					LRD = Double.parseDouble(value.substring(2));
				}
				// Send LRD from each neighbor to point
				else {
					p_list.add(value.substring(2));
					
				}
			}


			if (p_list.size() > 0) {
				for (int ir = 0; ir < p_list.size(); ir++) {
					point = p_list.get(ir);
					textKey.set(point);
					textValue.set(Double.toString(LRD));
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

		if (args.length != 3) {
			System.err.println("Usage: Step5 (HDFS point and neighbor file) (HDFS point and LRD) (HDFS output file)");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Step 5");

		job.setJarByClass(Step5.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NeighborMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LRDMapper.class);

		boolean success = job.waitForCompletion(true);
	}
}
