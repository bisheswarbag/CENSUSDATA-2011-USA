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

public class TotalScholarship2 {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// without x value
			String[] str = value.toString().split(",");
			String part1 = str[0];
			String[] p1 = part1.split(" ");
			int age = Integer.parseInt(p1[1]);

			String par = str[6];

			if (par.contains("Neither parent present")
					|| (par.contains(" Not in universe"))) {

				context.write(new Text("null"), new Text(age + ""));
			}

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int age = 0;
			String age1 = null;
			int p1 = 5000, p2 = 10000;
			int count1 = 0, count2 = 0;

			for (Text val : values) {
				String[] rec = val.toString().split(",");
				age1 = rec[0];

				// new age
				age = Integer.parseInt(age1);

				if ((age <= 12)) {
					count1++;

				} else if ((age >= 13) && (age <= 17)) {
					count2++;
				}

			}

			String ScholarshipAmt1 = " Total Scholarship for : " + count1
					+ " Infants(0 to 12) Amount is " + p1 + " Total amount "
					+ (count1 * p1);
			String ScholarshipAmt2 = " Total Scholarship for : " + count2
					+ " Teenagers(13 to 17) Amount is " + p2 + " Total amount "
					+ (count2 * p2);

			context.write(null, new Text(ScholarshipAmt1));
			context.write(null, new Text(ScholarshipAmt2));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(TotalScholarship2.class);
		job.setJobName("Total Scholarship amount for orphans");
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
