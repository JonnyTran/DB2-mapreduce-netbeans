import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class AvgSalary {

    public static class AvgSalaryMapper extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        public void map(Object key,Text value, Context c) throws IOException, InterruptedException{
            
            try {
                String str = value.toString();
                String[] strList = str.split(",");
                
                String year = strList[0].substring(0, 4);
                String state = strList[5];
                String sex = strList[69];                
                float wage = Float.valueOf(strList[72]) / 10;
                
                if (!year.isEmpty() && !state.isEmpty() && !sex.isEmpty() && wage > 0) {
                    c.write(new Text(year + " " + state + " " + sex), new FloatWritable(wage));
                }
                
            } catch (Exception e) {

            }
        }
    }

    public static class AvgSalaryPartitioner extends Partitioner<Text, FloatWritable>{
        @Override
        public int getPartition(Text key, FloatWritable value, int noOfReducers) {
            
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

    public static class AvgSalaryReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context c) throws IOException,
        InterruptedException {
            float sum = 0;
            int count = 0;
            
            for(FloatWritable wage: values){
                count++;
                sum += wage.get();
            }
            
            if (count > 0 && sum > 0) {
                c.write(key, new FloatWritable(sum / count));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException,
    ClassNotFoundException{
        Configuration conf = new Configuration();
        Job j2 = new Job(conf,"Average salary of male and female, of state & year Job");
        j2.setJarByClass(AvgSalary.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(FloatWritable.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);

        j2.setInputFormatClass(TextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);

        j2.setMapperClass(AvgSalaryMapper.class);
        j2.setReducerClass(AvgSalaryReducer.class);
        j2.setPartitionerClass(AvgSalaryPartitioner.class);
        
        j2.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(j2, new Path(args[1]));
        FileInputFormat.addInputPath(j2, new Path(args[0]));
        j2.waitForCompletion(true);
    }
}