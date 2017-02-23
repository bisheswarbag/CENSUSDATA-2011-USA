package bigDataExplain;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PensionAnalysis {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		private Configuration conf;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			conf = context.getConfiguration();
			String v = conf.get("value");
			int i = Integer.parseInt(v);
			String[] str = value.toString().split(",");
			String part1 = str[0];
			String[] p1 = part1.split(" ");

			int age = Integer.parseInt(p1[1]);
			int age1 = age + i;
			if (age1 >= 60) {
				context.write(new Text("" + age1), new Text(str[0] + "," + age1
						+ "," + i));
			}

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int pcount = 0;
			int nopen = 0;
			int xvalue = 0;
			int age = 0;
			int sum = 0;
			String age1 = null;
			String s1 = null;
			for (Text val : values) {
				String[] rec = val.toString().split(",");

				s1 = rec[0];

				age1 = rec[1];
				age = Integer.parseInt(age1);

				String xvalue1 = rec[2];
				xvalue = Integer.parseInt(xvalue1);

				pcount++;
				sum = sum + pcount;

			}
			// Count of People Capable for Pension after Years are :
			String PensionAva = " " + pcount;
			// String PensionNotAva =
			// " Count of People Not Capable for Pension after " + xvalue +
			// " Years are :  " + nopen;
			// context.write(new Text(" "), new Text(PensionAva));
			// context.write(new Text(" "), new Text(PensionNotAva));

			context.write(new Text(""), new Text(sum + ""));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		@SuppressWarnings("resource")
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the value of x years");
		int x;
		x = sc.nextInt();

		conf.setInt("value", x);
		Job job = Job.getInstance(conf);

		job.setJarByClass(PensionAnalysis.class);
		job.setJobName("Reduce Side Join");
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