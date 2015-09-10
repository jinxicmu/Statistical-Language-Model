package statisticalModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatisticalLanguageModel {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text keyP = new Text();
		private Text countP = new Text();
		private Text keyW = new Text();
		private Text countW = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] str = line.split("\t");
			String[] word = str[0].split(" ");
			int c = Integer.parseInt(str[1]);
			if (c >= 5) {
				StringBuilder outP0 = new StringBuilder();
				StringBuilder outP1 = new StringBuilder();
				StringBuilder outW0 = new StringBuilder();
				StringBuilder outW1 = new StringBuilder();

				outP0.append(str[0]);
				outP1.append("P ");
				outP1.append(str[1]);

				for (int i = 0; i < word.length - 1; i++){
					outW0.append(word[i]);
					outW0.append(" ");
				}
				outW1.append("W ");
				outW1.append(word[word.length - 1]);
				outW1.append(" ");
				outW1.append(str[1]);

				keyP.set(outP0.toString());
				countP.set(outP1.toString());
				keyW.set(outW0.toString().trim());
				countW.set(outW1.toString());
				if (word.length == 1)
					context.write(keyP, countP);
				else if (word.length == 5)
					context.write(keyW, countW);
				else {
					context.write(keyP, countP);
					context.write(keyW, countW);
				}
			}
		}
	}

	public static class Reduce extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		private int Nremain = 5;
		static byte[] family = Bytes.toBytes("i");

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// String line = values.toString();
			// StringTokenizer tokenizer = new StringTokenizer(line);
			List<Node> words = new ArrayList<Node>();
			int phaseCount = 1;
			for (Text val : values) {
				String tmp = val.toString();
				String[] str = tmp.split(" ");
				if (str[0].equals("P"))
					phaseCount = Integer.parseInt(str[1]);
				if (str[0].equals("W")) {
					Node nd = new Node(Integer.parseInt(str[2]), str[1]);
					words.add(nd);
				}
			}
			Collections.sort(words, new Comparator<Node>() {
				public int compare(Node o1, Node o2) {
					return o2.count - o1.count;// descending
				}
			});
			
			byte[] bRowKey = Bytes.toBytes(key.toString());
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
			Put put = new Put(bRowKey);

			for(int i = 0; i < Nremain && i < words.size(); i++){
				String colname = words.get(i).word;
				double possibility = words.get(i).count * 1.0 / phaseCount;
				String colval = Double.toString(possibility);
				put.add(family, Bytes.toBytes(colname),Bytes.toBytes(colval));	
			}
			if(!put.isEmpty())
				context.write(rowKey,put);
		}
	}

	public static class Node {
		int count;
		String word;

		Node() {
			count = 0;
			word = null;
		}

		Node(int c, String w) {
			count = c;
			word = w;
		}
	}

	public static void main(String[] args) throws Exception {
		String tableName = "langModel";
		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf, "SLM");
		job.setJarByClass(StatisticalLanguageModel.class);

		TableMapReduceUtil.initTableReducerJob(tableName, Reduce.class, job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
