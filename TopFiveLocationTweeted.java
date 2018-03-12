import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import java.util.HashMap;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopFiveLocationTweeted {

    public static class ReadCsvLine {
		public static String[] splitLine (String csv){
			
			String[] tokens = csv.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String[] results = new String[tokens.length];
	        for(int i = 0; i < tokens.length; i++){
	           results[i] = tokens[i].replace("\"", "");
	        }
	        return results;
		}         
    }

	public static class WCTweetedMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			String[] tokens = ReadCsvLine.splitLine(value.toString());

			if (tokens[0].isEmpty() || tokens[2].isEmpty() || tokens[0].equals("Handle")) 			{
				// skip this record
				return;
			}

			// The foreign join key is the user ID
			outkey.set(tokens[0]);

			// Flag this record for the reducer and then output
			outvalue.set("A" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class WCUserMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			String[] tokens = ReadCsvLine.splitLine(value.toString());

			if (tokens[0].isEmpty() || tokens[1].isEmpty() || tokens[0].equals("Handle")) {
				// skip this record
				return;
			}
			// The foreign join key is the user ID
				outkey.set(tokens[0]);
			// Flag this record for the reducer and then output
				outvalue.set("B" + value.toString());
				context.write(outkey, outvalue);
		}
	}

	public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		private Text handle = new Text();
		private Text result = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			// Clear our lists
			listA.clear();
			listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					listA.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'B') {
					listB.add(new Text(t.toString().substring(1)));
				}
			}

			// Execute our join logic now that the lists are filled
			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException,
				InterruptedException {
			
				// right outer join
			for (Text B : listB) {
				String[] location = ReadCsvLine.splitLine(B.toString());
				String loc = location[4] + ", ";
				// If list A is not empty, join A and B
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						String[] tks = ReadCsvLine.splitLine(A.toString());
						String tweet = tks[1];
						
						handle.set(tweet);
						result.set(loc);
						context.write(result, handle);
					}
				} else {
					// Else, output B by itself
					return;
				}
			}
		}
	}
	
	//Mapper and Reducer to location and username only
	public static class LocationMapper extends
			Mapper<Object, Text, Text, IntWritable> {
				
		// Our output key and value Writables
		private Text handle = new Text();
	    private IntWritable result = new IntWritable();
		

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] tokens = ReadCsvLine.splitLine(value.toString());
			
			if (tokens[0].isEmpty() || tokens[1].isEmpty() || tokens.length > 6) {
				// skip this record
				return;
			}
			
			String location = tokens[0];
			
			handle.set(location);
			result.set(1);
			context.write(handle, result);
		}
	}
	
	public static class LocationReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
			
			private IntWritable result = new IntWritable();
			private Text handle = new Text();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String[] tokens = ReadCsvLine.splitLine(values.toString());
			
			// Initialize our result
			int sum = 0;
	        for (IntWritable val : values) {
	          sum += val.get();
	        }
			String location = key.toString() + ",";
			handle.set(location);
	        result.set(sum);
	        context.write(handle, result);

		}
	}
	
	public static class TweetCountMapper extends
			Mapper<Object, Text, NullWritable, Text> {
				
		// Our output key and value Writables
		// private Text handle = new Text();
// 		private UserTuple outTuple = new UserTuple();	
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] tokens = ReadCsvLine.splitLine(value.toString());
			
				// if (tokens[0].isEmpty() || tokens[1].isEmpty()) {
				// 	// skip this record
				// 	return;
				// }
				String topLoc = tokens[1].trim();
				
				// System.out.println("float: " + countWord);
	// 			System.out.println("sumUser: " + sumUser);
				
				repToRecordMap.put(Integer.parseInt(topLoc), new Text(value));

				if (repToRecordMap.size() > 5) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}			 
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	/* Binning Mapper  */
	

	/*Finding top 5 */
	public static class TweetCountReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String[] tokens = ReadCsvLine.splitLine(value.toString());

				repToRecordMap.put(Integer.parseInt(tokens[1].trim()),
						new Text(value));

				if (repToRecordMap.size() > 5) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Not enough input");
			System.exit(2);
		}
		Path outputDir = new Path(otherArgs[2]);
		
		Path outputDirIntermediate = new Path(otherArgs[2] + "Join");
		Path outputDirJoin = new Path(otherArgs[2] + "Join/part-r-00000");
		Path outputLocation = new Path(otherArgs[2] + "Location");
		Path outputLocationPath = new Path(otherArgs[2] + "Location/part-r-00000");
		Path outputTop = new Path(otherArgs[2] + "Top");

		Job job = new Job(conf, "Reduce Side Join");

		// Configure the join type
		job.setJarByClass(TopFiveLocationTweeted.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, WCTweetedMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, WCUserMapper.class);

		job.setReducerClass(UserJoinReducer.class);

		TextOutputFormat.setOutputPath(job, outputDirIntermediate);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		Job job2 = new Job(conf, "Tweets Count per location");
		job2.setJarByClass(TopFiveLocationTweeted.class);
		job2.setMapperClass(LocationMapper.class);
		//job.setCombinerClass(WCTweetedCombiner.class);
		job2.setReducerClass(LocationReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(job2, outputDirJoin);
		TextOutputFormat.setOutputPath(job2, outputLocation);
		job2.waitForCompletion(true);
		
	
		
		Job top = new Job(conf, "Top 5 Job");
		top.setJarByClass(TopFiveLocationTweeted.class);
		top.setMapperClass(TweetCountMapper.class);
		top.setCombinerClass(TweetCountReducer.class);
		top.setReducerClass(TweetCountReducer.class);
		top.setInputFormatClass(TextInputFormat.class);
		top.setOutputKeyClass(NullWritable.class);
		top.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(top, outputLocationPath);
		TextOutputFormat.setOutputPath(top, outputTop);
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
		top.waitForCompletion(true) ;
		System.exit(1);
	}
}
