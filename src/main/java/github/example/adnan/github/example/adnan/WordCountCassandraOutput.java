package github.example.adnan.github.example.adnan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This counts the occurrences of words in all files from directory and push the
 * results to Cassandra!
 *
 * For each word, we output the total number of occurrences across all body
 * texts.
 *
 * When outputting to Cassandra, result goes to the output_words column family:
 * 
 * CREATE TABLE cql3_worldcount.output_words ( word text PRIMARY KEY, count_num
 * text )
 */
public class WordCountCassandraOutput extends Configured implements Tool {

	static final String KEYSPACE = "cql3_worldcount";
	static final String OUTPUT_COLUMN_FAMILY = "output_words";
	private static final String PRIMARY_KEY = "word";
	public static final String INPUT_DIR_PATH = "/home/aubuntu/wordcount/input";

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new WordCountCassandraOutput(),
				args);
		System.exit(0);
	}

	public static class MapperFS extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class ReducerToCassandra
			extends
			Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
		private Map<String, ByteBuffer> keys;
		private ByteBuffer key;

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			keys = new LinkedHashMap<String, ByteBuffer>();

		}

		@Override
		public void reduce(Text word, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			for (IntWritable val : values)
				sum += val.get();

			variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));
			keys.put("word", ByteBufferUtil.bytes(word.toString()));
			context.write(keys, variables);
		}
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "wordcount");
		job.setJarByClass(WordCountCassandraOutput.class);
		job.setMapperClass(MapperFS.class);
		job.setReducerClass(ReducerToCassandra.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Map.class);
		job.setOutputValueClass(List.class);
		job.setOutputFormatClass(CqlOutputFormat.class);

		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE,
				OUTPUT_COLUMN_FAMILY);
		job.getConfiguration().set(PRIMARY_KEY, "word");
		String query = "update " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY
				+ " set count_num=?";
		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(),
				"localhost");
		ConfigHelper.setOutputPartitioner(job.getConfiguration(),
				"Murmur3Partitioner");

		job.setInputFormatClass(TextInputFormat.class);
		Path inputPath = new Path(INPUT_DIR_PATH);
		FileInputFormat.setInputPaths(job, inputPath);

		job.waitForCompletion(true);
		return 0;
	}
}