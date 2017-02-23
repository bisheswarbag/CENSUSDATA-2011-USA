package bigDataExplain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryEmployedUnemployed {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {

			String record = value.toString();
			String[] parts = record.split(",");
			String edu = parts[1];

			String weeksworked = parts[9];
			context.write(new Text(edu), new Text(weeksworked));

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int unemploy = 0;
			int employ = 0;
			for (Text val : values) {
				String str = val.toString();
				String[] s1 = str.split(":");
				String weekw = s1[1];
				if (weekw.contains(" 0}")) {
					unemploy++;
				} else
					employ++;

			}
			context.write(key, new Text(" No. of Unemployed " + unemploy
					+ "         No. of Employed " + employ));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CategoryEmployedUnemployed.class);
		job.setJobName("Count of Employed and employed based on education");
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