package org.hackreduce.examples.ngram.two_gram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.ngram.TwoGramMapper;
import org.hackreduce.models.ngram.TwoGram;

import java.io.IOException;

/**
 * @author Pier-Luc Caron St-Pierre <pl@tastyapp.org>
 */
public class NgramWordMatchCounterLowerCase extends Configured implements Tool {

    public enum Count {
        WORD_COUNT
    }

    public static class RecordCounterMapper extends TwoGramMapper<Text, LongWritable> {

        // Our own made up key to send all counts to a single Reducer, so we can
        // aggregate a total value.
        public static final Text TOTAL_COUNT = new Text("total");

        // Just to save on object instantiation
        public static final LongWritable ONE_COUNT = new LongWritable(1);

        @Override
        protected void map(TwoGram record, Context context) throws IOException, InterruptedException {

            Text gram = new Text(record.getGram1().toLowerCase() + " " + record.getGram2().toLowerCase());
            LongWritable longWritable = new LongWritable(record.getMatchCount());

            context.write(gram, longWritable);
        }
    }

    public static class CounterReducter extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            Long totalCount = 0L;

            for (LongWritable longWritable : values) {
                totalCount += longWritable.get();
            }

            LongWritable longWritableCount = new LongWritable(totalCount);

            System.out.println(key);
            System.out.println(longWritableCount);
            context.write(key, longWritableCount);
        }

    }


    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new NgramWordMatchCounterLowerCase(), args);
        System.exit(result);
    }

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
        job.setMapperClass(RecordCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        // This is what the Reducer will be outputting
        job.setReducerClass(CounterReducter.class);
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
}
