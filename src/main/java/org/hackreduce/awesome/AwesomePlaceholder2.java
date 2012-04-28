package org.hackreduce.awesome;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AwesomePlaceholder2 {

	public enum Count {
		TOTAL_RECORDS, UNIQUE_KEYS
	}

	public static class AwesomeReducer extends
			Reducer<Text, AwesomeWritable, Text, AwesomeWritable> {
		protected AwesomeWritable aw;

		protected void reduce(Text key, Iterable<AwesomeWritable> values,
				Context context) throws IOException, InterruptedException {
			aw = new AwesomeWritable(new LongWritable(0), new LongWritable(0));

			for (AwesomeWritable value : values) {
				aw.addMoreAwesomeStuff(value);
			}

			context.write(key, aw);
		}
	}

	public abstract static class AwesomeNGramMapper extends
			Mapper<LongWritable, Text, Text, AwesomeWritable> {

		protected AwesomeWritable aw = new AwesomeWritable(new LongWritable(),
				new LongWritable());

		protected String[] splittedValue = new String[3];

		protected Text outputKey = new Text();

		protected abstract AwesomeWritable getAwesomeWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			splittedValue = value.toString().split("\t");

			outputKey.set(splittedValue[0]);

			context.getCounter(Count.TOTAL_RECORDS).increment(1);

			context.write(outputKey, getAwesomeWritable());
		}
	}


	public static class AwesomeGoogleBooksMapper extends AwesomeNGramMapper {
		@Override
		protected AwesomeWritable getAwesomeWritable() {
			aw.setGoogleBooks(Long.parseLong(splittedValue[1]));
			return aw;
		}
	}

	public static class AwesomeWikipediaMapper extends AwesomeNGramMapper {
		@Override
		protected AwesomeWritable getAwesomeWritable() {
			aw.setWikipedia(Long.parseLong(splittedValue[1]));
			return aw;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "AwesomePlaceholder1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AwesomeWritable.class);

		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);

		// job.setNumReduceTasks(0);

		String typeOfJob = args[2].toLowerCase();

		if (typeOfJob.contains("google")) {
			job.setMapperClass(AwesomeGoogleBooksMapper.class);
			job.setNumReduceTasks(0);

		} else if (typeOfJob.contains("wikipedia")) {
			job.setMapperClass(AwesomeWikipediaMapper.class);
			job.setNumReduceTasks(0);

		} else if (typeOfJob.contains("both")) {
			//job.setMapperClass(AwesomeIdentityMapper.class);
			job.setReducerClass(AwesomeReducer.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AwesomeWritable.class);

		} else {
			throw new RuntimeException("Y U NO GIVE ME THE RIGHT PARAMETERS??");
		}

		// job.setReducerClass(Reduce.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
