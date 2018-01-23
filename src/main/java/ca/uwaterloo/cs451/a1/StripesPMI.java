package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfLongFloat;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfFloats;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.MapKF;
import tl.lin.data.map.HashMapWritable;

public class StripesPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StripesPMI.class);
	private static final String INTERM_PATH = "INTERM_PATH";
	private static final String MAPPER_RECORD_COUNT = "MAPPER_RECORD_COUNT";
	private static final int MAX_WORD = 40;
	private static final String THRESHOLD = "THRESHOLD";

	private static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Text WORD = new Text();
		private static final IntWritable ONE = new IntWritable(1);

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
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	private static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HMapStFW> {
		private static final Text KEY = new Text();
		private static final HMapStFW MAP = new HMapStFW();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = Tokenizer.tokenize(value.toString());

			int sentenceEnd = Math.min(MAX_WORD, tokens.size());
			tokens = tokens.subList(0, sentenceEnd);
			Set uniqueTokens = new HashSet<>(tokens);
			tokens = new ArrayList<>(uniqueTokens);
			for (int i = 0; i < tokens.size(); i++) {
				MAP.clear();
				KEY.set(tokens.get(i));
				for (int j = 0; j < tokens.size(); j++) {
					if (i == j || (tokens.get(i)).equals(tokens.get(j))) {
						continue;
					}
					MAP.put(tokens.get(j), 1f);
				}
				context.write(KEY, MAP);
			}

		}
	}

	private static final class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
		@Override
		public void reduce(Text key, Iterable<HMapStFW> values, Context context)
				throws IOException, InterruptedException {
			Iterator<HMapStFW> itr = values.iterator();
			HMapStFW map = new HMapStFW();
			while (itr.hasNext()) {
				map.plus(itr.next());
			}
			context.write(key, map);
		}
	}

	private static final class MyReducer2 extends Reducer<Text, HMapStFW, Text, HashMapWritable> {
		private static final HashMapWritable<Text, PairOfFloats> SUM = new HashMapWritable<Text, PairOfFloats>();
		private static final PairOfFloats PAIR = new PairOfFloats();
		private static String intermPath;
		private static Map<String, Integer> vocabSizeMap = new HashMap<>();
		private static long lineCount = 0l;
		private static int threshold = 10;

		public void setup(Context context) throws IOException {
			intermPath = context.getConfiguration().get(INTERM_PATH);
			lineCount = context.getConfiguration().getLong(MAPPER_RECORD_COUNT, 1l);
			threshold = context.getConfiguration().getInt(THRESHOLD, 10);
			LOG.info("Intermediate path read in reducer" + intermPath);
			LOG.info("lineCount read in reducer" + lineCount);
			LOG.info("Threshold read in reducer" + threshold);

			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path pathInter = new Path(intermPath + "/part-r-00000");
			BufferedReader br = null;
			FSDataInputStream fileSystemStream = null;
			InputStreamReader isr = null;

			try {
				fileSystemStream = fs.open(pathInter);
				isr = new InputStreamReader(fileSystemStream);
				br = new BufferedReader(isr);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				String line = "";
				while ((line = br.readLine()) != null) {
					String[] arr = line.split("\\s+");
					if (arr.length == 2) {
						vocabSizeMap.put(arr[0], Integer.parseInt(arr[1]));
					} else {
						LOG.info("Some error while creating vocabulary.");
					}
				}
			} catch (Exception e) {
				LOG.info("Some error occured while reading the intermediate output file");
			}

			br.close();

			LOG.info("In reduce setup Vocab size!:" + vocabSizeMap.size());

		}

		@Override
		public void reduce(Text key, Iterable<HMapStFW> values, Context context)
				throws IOException, InterruptedException {
			Iterator<HMapStFW> itr = values.iterator();
			HMapStFW map = new HMapStFW();
			while (itr.hasNext()) {
				map.plus(itr.next());
			}

			String leftElement = key.toString();
			String rightElement = null;
			float sum = 0f;
			SUM.clear();
			boolean emit = false;
			for (MapKF.Entry<String> entry : map.entrySet()) {
				rightElement = entry.getKey();
				sum = entry.getValue();
				if (sum >= threshold && null != vocabSizeMap && !vocabSizeMap.isEmpty()
						&& null != vocabSizeMap.get(leftElement) && null != vocabSizeMap.get(rightElement)) {
					float jointV = (float) Math.log10(sum);
					float count = (float) Math.log10(lineCount);
					float margLeft = (float) Math.log10((float) vocabSizeMap.get(leftElement));
					float margRight = (float) Math.log10((float) vocabSizeMap.get(rightElement));
					float pmi = jointV + count - margLeft - margRight;
					PairOfFloats pf = new PairOfFloats();
					pf.set(pmi, sum);
					SUM.put(new Text(rightElement), pf);
					emit = true;
				}
			}
			if (emit) {
				context.write(key, SUM);
			}

		}
	}

	private StripesPMI() {
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

		@Option(name = "-threshold", metaVar = "[num]", usage = "min cooccurrence count")
		int threshold = 10;

	}


	@Override
	public int run(String[] argv) throws Exception {
		final Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		LOG.info("Tool: " + StripesPMI.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - final output path: " + args.output);
		LOG.info(" - threshold: " + args.threshold);
		LOG.info(" - number of reducers: " + args.numReducers);
		LOG.info(" - text output: " + args.textOutput);

		/*The first job*/
		Job job1 = Job.getInstance(getConf());

		job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
		job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
		job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
		job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
		job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

		job1.setJobName(StripesPMI.class.getSimpleName());
		job1.setJarByClass(StripesPMI.class);

		job1.setNumReduceTasks(1);

		String inputPath = args.input;
		String intermOutputPath = "PairsStripesJob1Output";
		Path intermDir = new Path(intermOutputPath);
		FileSystem.get(getConf()).delete(intermDir, true);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(intermOutputPath));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setMapperClass(MyMapper.class);
		job1.setCombinerClass(MyReducer.class);
		job1.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);

		long mapperRecord = job1.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		System.out.println("New Way Counter" + mapperRecord);
		LOG.info("New Way Counter" + mapperRecord);
		System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		/*The first job*/
		Job job2 = Job.getInstance(getConf());
		job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
		job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
		job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
		job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
		job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
		job2.setJobName(StripesPMI.class.getSimpleName());
		job2.setJarByClass(StripesPMI.class);
		job2.getConfiguration().setInt(THRESHOLD, args.threshold);

		job2.getConfiguration().set(INTERM_PATH, intermOutputPath);
		job2.getConfiguration().setLong(MAPPER_RECORD_COUNT, mapperRecord);
		job2.setNumReduceTasks(args.numReducers);

		Path outputDir = new Path(args.output);
		FileSystem.get(getConf()).delete(outputDir, true);
		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(args.output));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(HMapStFW.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(HashMapWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setCombinerClass(MyCombiner.class);
		job2.setReducerClass(MyReducer2.class);

		// Delete the output directory if it exists already.
        FileSystem.get(getConf()).delete(outputDir, true);
        job2.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 *
	 * @param args
	 *            command-line arguments
	 * @throws Exception
	 *             if tool encounters an exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new StripesPMI(), args);
	}
}
