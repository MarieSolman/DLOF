package finalProj.hadoop;

import java.io.IOException;
import java.lang.Object.*;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Step1 {

public static class PtMapper extends Mapper<Object, Text, Text,
DoubleWritable> {

double maxDist = -1;
double distance;
double sumForDist = 0;
double maxDistPotential;
int kValue = 2;
ArrayList<Double> distList = new ArrayList<Double>();
List<Double> kNNDist = new ArrayList<Double>();
String maxDistStr = "";
ArrayList<String> pointList = new ArrayList<String>();


public void map(Object key, Text value, Context context) throws
IOException, InterruptedException {

String vString = value.toString();
pointList.add(vString);
System.out.println("PointList" + pointList);
System.out.println(vString + "vString");

}

@Override

public void cleanup(Context context) throws IOException,
InterruptedException {
System.out.println("PointList Cleanup" + pointList);
for (int i = 0; i < pointList.size(); i++) {
maxDist = 0;
kNNDist = new ArrayList<Double>();
distList.clear();
for (int j = 0; j < pointList.size(); j++) {
String[] p1 = pointList.get(i).split(","); //.split might not work
String[] p2 = pointList.get(j).split(",");
int x1 = Integer.parseInt(p1[0]);
int y1 = Integer.parseInt(p1[1]);
System.out.println("x1" + x1 + "y1" + y1);

int x2 = Integer.parseInt(p2[0]);
int y2 = Integer.parseInt(p2[1]);
System.out.println("x2" + x2 + "y2" + y2);
if(Arrays.equals(p1, p2)){
}
else{
distance = Math.hypot(x1-x2, y1-y2);
distList.add(distance);
}
Collections.sort(distList);
}
if(distList.size() < 2) {
        System.out.println("TOO SMALL" + Arrays.toString(distList.toArray()));
}
else{
        kNNDist = distList.subList(0, kValue);


for(double dist : kNNDist){
        if (dist > maxDist) {
        maxDist = dist;
}


}
}
}

    String toReducer = "Reduce";
    context.write(new Text(toReducer), new DoubleWritable(maxDist));

}
}




public static class Reduce extends Reducer<Text, DoubleWritable,
DoubleWritable, NullWritable> {
public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
throws IOException, InterruptedException {

double maxDist = 0;

    for (DoubleWritable val : values) {
        if(val.get() > maxDist){
            maxDist = val.get();
        }
    }

context.write(new DoubleWritable(maxDist), NullWritable.get());

}


}


private static void setTextOutputFormatSeparator(Configuration conf,
String separator) {
conf.set("mapred.textoutputformat.separator", separator);
//PriortoHadoop 2 (YARN)
conf.set("mapreduce.textoutputformat.separator", separator); // Hadoopv2+ (YARN)
conf.set("mapreduce.output.textoutputformat.separator", separator);
conf.set("mapreduce.output.key.field.separator", separator);
conf.set("mapred.textoutputformat.separatorText", separator); // ?
}

@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {

Configuration conf = new Configuration();
setTextOutputFormatSeparator(conf, ",");
Job job = Job.getInstance(conf);

job.setJarByClass(Step1.class);
job.setReducerClass(Reduce.class);
job.setMapOutputKeyClass(Text.class);
job.setMapperClass(PtMapper.class);
job.setMapOutputValueClass(DoubleWritable.class);
job.setOutputKeyClass(DoubleWritable.class);
job.setOutputValueClass(NullWritable.class);


FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));


System.exit(job.waitForCompletion(true) ? 0 : 1);
conf.set("mapred.task.timeout", "6000000");
}
}
