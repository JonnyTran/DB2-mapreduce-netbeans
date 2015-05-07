import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
/**
*
* @author Soumyava
*/
public class CustomPartitioner {

	public static class PartMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key,Text value, Context c) throws IOException, InterruptedException{
			String[] strList = value.toString().split(",");
			String gender = strList[2];
			String nameAgeScore = strList[0]+","+strList[1];
			c.write(new Text(gender), new Text(nameAgeScore));
		}
	}

	public static class PartPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int noOfReducers) {
			int age = Integer.parseInt(value.toString().split(",")[1]);
			if(noOfReducers == 0)
				return 0;
			if(age <= 20){
				return 0;
			}
			else if (age >20 && age <=50){
				return 1%noOfReducers;
			}
			else
				return 2%noOfReducers;
		}

	}

	public static class PartReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context c) throws IOException,
		InterruptedException {
			for(Text val: values){
				c.write(key,val);
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		Job j2 = new Job(conf,"Custom Partitioner");
		j2.setJarByClass(CustomPartitioner.class);

		j2.setMapOutputKeyClass(Text.class);
		j2.setMapOutputValueClass(Text.class);

		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(Text.class);

		j2.setInputFormatClass(TextInputFormat.class);
		j2.setOutputFormatClass(TextOutputFormat.class);

		j2.setMapperClass(PartMapper.class);
		j2.setReducerClass(PartReducer.class);
		j2.setPartitionerClass(PartPartitioner.class);
		j2.setNumReduceTasks(3);
		FileOutputFormat.setOutputPath(j2, new Path(args[1]));
		FileInputFormat.addInputPath(j2, new Path(args[0]));
		j2.waitForCompletion(true);
	}
}