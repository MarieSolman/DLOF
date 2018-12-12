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

public class Step6 {

	public static class NeighborLRDMapper extends Mapper<Object, Text, Text, Text> {
		@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Mapper 2: Import Point / LRD of neighbors, send to reducer basically as is

			String line = value.toString();
			String[] tokens = line.split("\\s+");
			String outputKey, outputValue;
			Text textKey = new Text();
			Text textValue = new Text();
			double LRD;

			// Get point
			String point = tokens[0];

			// Get LRD
			LRD = Double.parseDouble(tokens[1]);
			
			// send point and LRD to reducer
			textKey.set(point);
			textValue.set("n," + Double.toString(LRD));
			context.write(textKey, textValue);
			
		}
	}

	public static class LRDMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Mapper 2: Import Point / LRD of self, send to reducer basically as is

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
			textValue.set("s," + Double.toString(LRD));
			context.write(textKey, textValue);
		}
	}

	// Reducer computes outliers for each segment, corresponding to a square of r to a side
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Reducer 2: receive point, its own LRD, and LRDs of all neighbors.  Compute average of neighbor LRDs and 
			// divide by own LRD to get LOF.  Output point, LRD

			double LRD = 0;
			double LRDTot = 0;
			double LOF = 0;
			double count = 0;
			Text textValue = new Text();

			for (Text val : values) {
				String value = val.toString();
				// Take point's LRD and set it aside, note that LRD sorts first
				// so this will be first in the array
				if (value.charAt(0) == 's') {
					LRD = Double.parseDouble(value.substring(2));
				}
				// Send LRD from each neighbor to point
				else {
					LRDTot += Double.parseDouble(value.substring(2));
					count++;
				}
			}

			LOF = LRDTot / count / LRD;

			textValue.set(Double.toString(LOF));
			context.write(key,textValue);

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
			System.err.println("Usage: Step6 (HDFS point and LRD of neighbors) (HDFS point and LRD of self) (HDFS output file)");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Step 6");

		job.setJarByClass(Step6.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NeighborLRDMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LRDMapper.class);

		boolean success = job.waitForCompletion(true);
	}
}
