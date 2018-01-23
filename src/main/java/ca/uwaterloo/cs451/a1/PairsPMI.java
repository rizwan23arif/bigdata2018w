package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfStrings;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	private static final int MAX_WORD = 40;

    private static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final Text WORD = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            ArrayList<String> uniqueWord = new ArrayList<>();
            int sentenceEnd = Math.min(MAX_WORD, tokens.size());
			
            for (int i = 0; i < sentenceEnd; i++) {
                String word = tokens.get(i);
                if (!uniqueWord.contains(word)) {
                    uniqueWord.add(word);
                }
            }
            for (int i = 0; i < uniqueWord.size(); i++) {
                String word = uniqueWord.get(i);
                WORD.set(word);
                context.write(WORD, ONE);
            }

            WORD.set("*");
            context.write(WORD, ONE);
        }
    }

    private static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
        private static final FloatWritable ONE = new FloatWritable(1);
        private static final PairOfStrings Pair = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            ArrayList<String> uniqueWord = new ArrayList<>();

            int sentenceEnd = Math.min(MAX_WORD, tokens.size());

            for (int i = 0; i < sentenceEnd; i++) {
                String word = tokens.get(i);
                if (!uniqueWord.contains(word)) {
                    uniqueWord.add(word);
                }
            }

            for (int i = 0; i < uniqueWord.size() - 1; i++) {
                for (int j = i + 1; j < uniqueWord.size(); j++) {
                    Pair.set(uniqueWord.get(i), uniqueWord.get(j));
                    context.write(Pair, ONE);

                    Pair.set(uniqueWord.get(j), uniqueWord.get(i));
                    context.write(Pair, ONE);
                }
            }
        }
    }

    private static final class MyCombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0f;
			for (FloatWritable floatVal : values) {
				sum += floatVal.get();
			}
			SUM.set(sum);
			context.write(key, SUM);
        }
    }

    private static final class MyReducer2 extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();
        private static HashMap<String, Integer> wordMap = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            String intermediatePath = conf.get("intermediatePath");
            Path inFile = new Path(intermediatePath + "/part-r-00000");

            if (!fs.exists(inFile)) {
                throw new IOException("File not found: " + inFile.toString());
            }

            BufferedReader reader = null;
            try {
                FSDataInputStream in = fs.open(inFile);
                InputStreamReader inStream = new InputStreamReader(in);
                reader = new BufferedReader(inStream);

            } catch (FileNotFoundException e) {
                throw new IOException("Failed to open file.");
            }

            LOG.info("Start reading file from " + intermediatePath);
            String line = reader.readLine();
            while (line != null) {
                String[] word = line.split("\\s+");
                if (word.length != 2) {
                    LOG.info("Input line is not valid: '" + line + "'");
                } else {
                    wordMap.put(word[0], Integer.parseInt(word[1]));
                }

                line = reader.readLine();
            }

            LOG.info("Finish reading file.");
            reader.close();

        }

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            float numThreshold = Float.parseFloat(conf.get("threshold"));
            float sum = 0.0f;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            if (sum >= numThreshold) {
                String leftWord = key.getLeftElement();
                String rightWord = key.getRightElement();
                Integer totalVal = wordMap.get("*");
                Integer leftVal = wordMap.get(leftWord);
                Integer rightVal = wordMap.get(rightWord);

                if (totalVal != null && leftVal != null && rightVal != null) {
                    float pmiVal = (float) Math.log10(1.0f * sum * totalVal / (leftVal * rightVal));

                    VALUE.set(pmiVal, (int) sum);
                    context.write(key, VALUE);
                }
            }
        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private PairsPMI() {
    }

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
        boolean textOutput = true;

        @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurence pairs")
        int numThreshold = 10;
    }

    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(200));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

    /*The first job*/
        String intermediatePath = args.output + "-tmp";
        //String intermediatePath = "cs489-2017w-lintool-a1-shakespeare-pairs-tmp";

        LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + intermediatePath);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);
        LOG.info(" - num threshold: " + args.numThreshold);

        Configuration conf = getConf();
        conf.set("threshold", Integer.toString(args.numThreshold));
        conf.set("intermediatePath", intermediatePath);

        Job job = Job.getInstance(getConf());
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(intermediatePath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        // Delete the output directory if it exists already.
        Path intermediateDir = new Path(intermediatePath);
        FileSystem.get(getConf()).delete(intermediateDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("The first job is finished.");

    /*The second job*/
        LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);
        LOG.info(" - num threshold: " + args.numThreshold);

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(PairsPMI.class.getSimpleName());
        job2.setJarByClass(PairsPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(FloatWritable.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(PairOfFloatInt.class);
        if (args.textOutput) {
            job2.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        job2.setMapperClass(MyMapper2.class);
        job2.setCombinerClass(MyCombiner.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setPartitionerClass(MyPartitioner.class);

        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job2.waitForCompletion(true);

        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}