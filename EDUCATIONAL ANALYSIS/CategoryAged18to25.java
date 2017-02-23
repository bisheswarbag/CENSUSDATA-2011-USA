package bigDataExplain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryAged18to25 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {

			String record = value.toString();
			String[] parts = record.split(",");
			String edu = parts[1];

			String age1 = parts[0];
			String[] age2 = age1.split(":");
			String age3 = age2[1];
			String[] age4 = age3.split(" ");
			String age5 = age4[1];
			int age = Integer.parseInt(age5);
			if (age >= 18 && age <= 25) {
				context.write(new Text(edu), new Text("" + age));
			}

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (@SuppressWarnings("unused")
			Text val : values) {
				count++;

			}
			String nop = " NO. Of People doiing " + key + " are :  " + count;
			context.write(new Text(" "), new Text(nop));

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CategoryAged18to25.class);
		job.setJobName("Count of people aged between 18 to 25 based  on education");
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