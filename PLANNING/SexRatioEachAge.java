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

public class SexRatioEachAge {

	// sex ratio for each age group
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] s = record.split(",");

			String[] s1 = s[0].split(":");
			String[] s2 = s1[1].split(" ");
			String age = s2[1];

			String[] s3 = s[3].split(":");
			String[] s4 = s3[1].split(" ");

			int a = s4[2].length() - 1;

			// char c=s4[2].charAt(s4[2].length() - 1);
			// System.out.print(c);
			String s5 = s4[2].substring(0, a);
			// System.out.print(s5);

			context.write(new Text("null"), new Text(s5 + "," + age));
		}
	}

	public static class ReduceClass extends
			Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long Age = 0;
			String sex;
			int male1 = 0, female1 = 0, male2 = 0, female2 = 0, male3 = 0, female3 = 0, male4 = 0, female4 = 0, male5 = 0, female5 = 0, male6 = 0, female6 = 0;
			for (Text val : values) {
				String parts = val.toString();
				String[] p = parts.split(",");
				Age = Long.parseLong(p[1]);
				sex = String.valueOf(p[0]);

				if ((Age >= 0) && (Age <= 12)) {
					if (sex.equals("Female")) {
						male1++;
					} else if (sex.equals("Male")) {
						female1++;
					}
				} else if ((Age >= 13) && (Age <= 17)) {
					if (sex.equals("Female")) {
						male2++;
					} else if (sex.equals("Male")) {
						female2++;
					}
				} else if ((Age >= 18) && (Age <= 40)) {
					if (sex.equals("Female")) {
						male3++;
					} else if (sex.equals("Male")) {
						female3++;
					}
				} else if ((Age >= 41) && (Age <= 60)) {
					if (sex.equals("Female")) {
						male4++;
					} else if (sex.equals("Male")) {
						female4++;
					}
				} else if ((Age >= 61) && (Age <= 80)) {
					if (sex.equals("Female")) {
						male5++;
					} else if (sex.equals("Male")) {
						female5++;
					}
				} else if ((Age >= 81) && (Age <= 100)) {
					if (sex.equals("Female")) {
						male6++;
					} else if (sex.equals("Male")) {
						female6++;
					}
				}

			}

			String a = "Sex Ratio of Infants (0 to 12) male:female " + male1
					+ ":" + female1;
			String b = "Sex Ratio of Teenegers (13 to 17) male:female " + male2
					+ ":" + female2;
			String c = "Sex Ratio of Adults (18 to 40) male:female " + male3
					+ ":" + female3;
			String d = "Sex Ratio of Middle Aged (41 to 60) male:female "
					+ male4 + ":" + female4;
			String e = "Sex Ratio of Seniors (61 to 80) male:female " + male5
					+ ":" + female5;
			String f = "Sex Ratio of Elderly (81 to 100) male:female " + male6
					+ ":" + female6;

			context.write(null, new Text(a));
			context.write(null, new Text(b));
			context.write(null, new Text(c));
			context.write(null, new Text(d));
			context.write(null, new Text(e));
			context.write(null, new Text(f));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SexRatioEachAge.class);
		job.setJobName("Sex Ratio of Each Age Group ");
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