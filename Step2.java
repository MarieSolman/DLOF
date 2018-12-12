package finalProj.hadoop;

import java.io.IOException;
import java.lang.Object.*;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;
import java.awt.geom.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Step2 {

public static class KNNMapper extends Mapper<Object, Text, Text, Text> {

double maxDist;

@Override
protected void setup(Context context) throws IOException, InterruptedException {
double getMaxDist;
Configuration conf = context.getConfiguration();
getMaxDist = Double.parseDouble(conf.get("maxDist", "20"));
maxDist = getMaxDist;
}

public void map(Object key, Text value, Context context) throws
IOException, InterruptedException {

int gridIDInt;
String flag = "";

System.out.println(maxDist + "maxDist");
int gridSweep = (int)maxDist / 100 + 1;
System.out.println(gridSweep + "gridSweep");

String line = value.toString();
String[] tokens = line.split(",");
String xPtStr = tokens[0];
String yPtStr = tokens[1];
int xPt = Integer.parseInt(xPtStr);
int yPt = Integer.parseInt(yPtStr);

int gridIDX = (xPt < 100) ? 1 :
Integer.parseInt(Integer.toString(xPt).substring(0, 1));
int gridIDY = (yPt < 100) ? 1 :
Integer.parseInt(Integer.toString(yPt).substring(0, 1));

String baseGridIDStr = ("X"+Integer.toString(gridIDX) + " Y" +
Integer.toString(gridIDY));

context.write(new Text(baseGridIDStr), new Text(line + ",Full"));
String gridIDStr;
String testGridIDStr;
int suppIDX;
int suppIDY;
int suppIDLowerBound = gridSweep * -1;
int suppIDUpperBound = gridSweep;
flag = "Supp";
System.out.println(gridSweep);
for(int xIndex = suppIDLowerBound; xIndex <= suppIDUpperBound;xIndex ++){
for(int yIndex = suppIDLowerBound; yIndex <= suppIDUpperBound; yIndex ++){


suppIDX = gridIDX + xIndex;
suppIDY = gridIDY + yIndex;
if((suppIDX < 1) || (suppIDX > 10)){
}
else if((suppIDY < 1) || (suppIDY > 10)){
}
else if((xIndex == 0) && (yIndex == 0)){
}
else{
gridIDStr = ("X"+Integer.toString(suppIDX) + " Y" + Integer.toString(suppIDY));
testGridIDStr = ("original " + baseGridIDStr +
"X"+Integer.toString(suppIDX) + " Y" + Integer.toString(suppIDY));
System.out.println("point " + line + " XXXX " + gridIDStr);
//System.out.println("gridIDX " + gridIDX + " gridIDY " + gridIDY + "
" + gridIDStr);
context.write(new Text(gridIDStr), new Text(line +","+flag));
System.out.println(gridIDStr);
System.out.println(line + ","+flag);
}
}
}
}
/* if(i == 0 && iNest == 0){
}
else if(((gridIDX - i) < 1) || ((gridIDX + i) > 10)){
System.out.println(gridIDX + " out of scope");
}
else if(((gridIDY + iNest) < 1) || ((gridIDY + iNest) > 10)){
System.out.println(gridIDY + " out of scope");
}
else{
for(int xIndex = suppIDXLeftBound; i <= suppIDXRightBound; i++){
for(int yIndex =
gridIDStr = ("Y"+Integer.toString(suppIDY) + " X" + Integer.toString(suppIDX));
context.write(new Text(gridIDStr), new Text(line + ",Supp")); */
}
public static class Reduce extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
ArrayList<String> pointList = new ArrayList<String>();
Double pointDist;
double pointX;
double pointY;
double compPointX;
double compPointY;
int kValue = 2;
for (Text val: values)
{
String vString = val.toString();
pointList.add(vString);
//context.write(key, new Text(vString));
//System.out.println(key.toString());
//System.out.println(vString);
}
for (String point : pointList){
ArrayList<String> NNeighbors = new ArrayList<String>();
List<String> kNN = new ArrayList<String>();
String[] pVector = point.split(",");
if(pVector[2].equals("Supp")){
System.out.println(Arrays.toString(pVector));
System.out.println("SHould be only one");
continue;
}
pointX = Double.parseDouble(pVector[0]);
pointY = Double.parseDouble(pVector[1]);
String pointOut = "(" +pVector[0] + "," + pVector[1]+")";
System.out.println(Arrays.toString(pVector));
System.out.println(pVector[0]);
System.out.println(pVector[1]);
System.out.println(pVector[2]);
System.out.println(pointList);
for(String compPoint : pointList) {
String[] cPVector = compPoint.split(",");
compPointX = Double.parseDouble(cPVector[0]);
compPointY = Double.parseDouble(cPVector[1]);
pointDist = Point2D.distance(pointX, pointY, compPointX, compPointY);
String pointCompOut = cPVector[0] + "," + cPVector[1];
String NNOut = pointDist + "," + pointCompOut;
System.out.println(compPoint);
if(pointDist > 0) {
NNeighbors.add(NNOut);
}
}
Collections.sort(NNeighbors, new Comparator<String>() {
public int compare(String str1, String str2) {
String[] str1Vector = str1.split(",");
String[] str2Vector = str2.split(",");
return Double.compare(Double.parseDouble(str1Vector[0]),Double.parseDouble(str2Vector[0]));
}
});
if(NNeighbors.size() < 2){
System.out.println("TOO SMALL" + Arrays.toString(NNeighbors.toArray()));
}
else{
kNN = NNeighbors.subList(0, kValue);
StringBuffer sb = new StringBuffer();
String neighborOut = "";
for (String s: kNN) {
String[] kNNVector = s.split(",");
//System.out.println(kNNVector[0]);
//System.out.println(kNNVector[1]);
//System.out.println(kNNVector[1] + "," + kNNVector[2]);
neighborOut = "("+kNNVector[1] + "," + kNNVector[2]+")";
sb.append(neighborOut);
sb.append(" ");
}
String kNNStr = sb.toString();
//System.out.println(pointOut + "pointOut");
//System.out.println(kNNStr + "knnStr");
context.write(new Text(pointOut), new Text(kNNStr));
}
}
}
}
private static void setTextOutputFormatSeparator(Configuration conf,
String separator) {
conf.set("mapred.textoutputformat.separator", separator); // Prior toHadoop 2 (YARN)
conf.set("mapreduce.textoutputformat.separator", separator); // Hadoop v2+ (YARN)
conf.set("mapreduce.output.textoutputformat.separator", separator);
conf.set("mapreduce.output.key.field.separator", separator);
conf.set("mapred.textoutputformat.separatorText", separator); // ?
}
@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
setTextOutputFormatSeparator(conf, " ");
Job job = Job.getInstance(conf);
job.setJarByClass(Step2.class);
job.setReducerClass(Reduce.class);
job.setMapperClass(KNNMapper.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
conf.set("maxDist", args[2]);
System.exit(job.waitForCompletion(true) ? 0 : 1);
conf.set("mapred.task.timeout", "6000000");
}
}
