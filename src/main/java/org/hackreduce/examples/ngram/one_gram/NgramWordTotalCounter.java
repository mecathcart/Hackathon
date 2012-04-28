package org.hackreduce.examples.ngram.one_gram;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.ngram.OneGramMapper;
import org.hackreduce.models.ngram.OneGram;


/**
 * This MapReduce job will count the total number of {@link OneGram} records in the data dump.
 *
 */
public class NgramWordTotalCounter extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 2) {
            System.err.println("Usage: " + getClass().getName() + " <input> <output>");
            System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(WordTotalCounterMapper.class);
        job.setReducerClass(WordTotalCounterReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        // This is what the Mapper will be outputting to the Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // This is what the Reducer will be outputting
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Setting the input folder of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static final Text TOTAL_COUNT = new Text("total");

    public static class WordTotalCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        // Our own made up key to send all counts to a single Reducer, so we can
        // aggregate a total value.

        // Just to save on object instantiation
        public static final LongWritable ONE_COUNT = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(TOTAL_COUNT, new LongWritable(1));
        }
    }

    public static class WordTotalCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long myCounter = 0L;

            for (LongWritable myLong : values) {
                myCounter++;
            }


            context.write(new Text("Total"), new LongWritable(myCounter));
        }
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NgramWordTotalCounter(), args);
        System.exit(result);
    }

}
