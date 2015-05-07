import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
*
* @author Soumyava
*/
public class AgeHistogram {

    public static class AgeMapper extends Mapper<Object,Text,Text,IntWritable> {
        @Override
        public void map(Object key, Text value, Context c) throws IOException, InterruptedException{
            String str = value.toString();
            String[] strList = str.split(",");

            String year = strList[0].substring(0, 4);
            String age = strList[8];

            c.write(new Text(year + " " + age), new IntWritable(1));
        }
    }

    public static class AgeReducer extends Reducer<Text,IntWritable,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable>values, Context c) throws IOException,InterruptedException{
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
            }
            c.write(key, new Text(""+count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job=  new Job(conf);

        job.setJobName("Age Histogram job");
        job.setJarByClass(AgeHistogram.class);

        //Mapper input and output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Reducer input and output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //file input and output of the whole program
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the mapper class
        job.setMapperClass(AgeMapper.class);

        //set the combiner class for custom combiner
        //j2.setCombinerClass(AgeReducer.class);
        //Set the reducer class
        job.setReducerClass(AgeReducer.class);

        //set the number of reducer if it is zero means there is no reducer
        //j2.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
    }
}