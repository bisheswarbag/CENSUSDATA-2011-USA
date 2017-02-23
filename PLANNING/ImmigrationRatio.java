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

public class ImmigrationRatio {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split(",");
			String cob = str[7];
			String weekswork = str[9];
			String citz = str[8];

			context.write(new Text("null"), new Text(cob + "," + weekswork
					+ "," + citz));

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int foreignworkers = 0, citizenworkers = 0;

			for (Text val : values) {
				String[] rec = val.toString().split(",");

				String weeksworkd = rec[1];
				@SuppressWarnings("unused")
				String cob = rec[0];
				String citizenship = rec[2];

				if ((!citizenship
						.contains(" Native- Born in the United States"))
						&& (!citizenship
								.contains(" Foreign born- U S citizen by naturalization"))) {
					if (!weeksworkd.contains(" 0}")) {
						foreignworkers++;
					}
				} else {
					if (!weeksworkd.contains(" 0}")) {
						citizenworkers++;
					}
				}

			}

			String citizentoimmigration = " No. of Citizen Employers : No. of Foreign Employers              "
					+ citizenworkers + " : " + foreignworkers;

			context.write(null, new Text(citizentoimmigration));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ImmigrationRatio.class);
		job.setJobName("Citizen To Immigration Ratio in Employment");
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
