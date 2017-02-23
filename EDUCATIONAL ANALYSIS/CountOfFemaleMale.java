package bigDataExplain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountOfFemaleMale {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			String edu = parts[1];

			String gender = parts[3];
			String[] MorF = gender.split(":");
			String[] MorF2 = MorF[1].split(" ");

			context.write(new Text(edu), new Text(MorF2[2]));

		}

	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int f1 = 0;
			int m1 = 0;

			for (Text val : values) {
				String s1 = val.toString();
				if (s1.contains("Female")) {
					f1++;
				} else {
					m1++;
				}

			}
			String female = "No. of Females : " + f1;
			String male = "No. of Males : " + m1;
			context.write(key, new Text("             " + female + "  " + male));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CountOfFemaleMale.class);
		job.setJobName("Count of Female & Male based on education");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
