import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
public class EquidepthAge {

    public static class EquidepthAgeMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context c) throws IOException, InterruptedException {
            String str = value.toString();
            String[] strList = str.split(",");
            
            try {
                String year = strList[0].substring(0, 4);
                int age = Integer.valueOf(strList[8]) / 10;
            
                c.write(new Text(year + " " + age), new IntWritable(1));                
            } catch (Exception e) {
                
            }
        }
    }

    public static class EquidepthAgePartitioner extends Partitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int noOfReducers) {
            
//            try {
//                String year = value.toString().split(",")[0];
//                    
//                if(noOfReducers == 0)
//                    return 0;
//
//                switch (year) {
//                    case "2009":
//                        return 0 % noOfReducers;
//                    case "2010":
//                        return 1 % noOfReducers;
//                    case "2011":
//                        return 2 % noOfReducers;
//                    case "2012":
//                        return 3 % noOfReducers;
//                    case "2013":
//                        return 4 % noOfReducers;
//                    default:
//                        return 0;
//                }
//            } catch (Exception e) {
//                return 0;
//            }
            return 0;
        }
    }

    public static class EquidepthAgeReducer extends Reducer<Text,IntWritable,Text,Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context c) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
            }
            c.write(key, new Text(""+count));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException,
    ClassNotFoundException{
        Configuration conf = new Configuration();
        Job j2 = new Job(conf,"Equidepth Age Job");
        j2.setJarByClass(EquidepthAge.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(IntWritable.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        j2.setInputFormatClass(TextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);

        j2.setMapperClass(EquidepthAgeMapper.class);
        j2.setReducerClass(EquidepthAgeReducer.class);
        j2.setPartitionerClass(EquidepthAgePartitioner.class);
        
        j2.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(j2, new Path(args[1]));
        FileInputFormat.addInputPath(j2, new Path(args[0]));
        j2.waitForCompletion(true);
    }
}