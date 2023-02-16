import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StartLetter {
    
    // Mapper -> classes tussen <> zijn de classen van de (input key, input_value, output_key, output_value)
    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
        
        // hier komt de mapfunctie in
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // map functie leest lijn per lijn
            // lijn splitsen in woorden
            // hello world
            StringTokenizer itr = new StringTokenizer(value.toString());
            // itr = [hello, world]
            while(itr.hasMoreTokens()){
                // hello -> (h, 1)
                // worlds -> (w, 1)
                String woord = itr.nextToken();
                woord = woord.toLowerCase();
                char firstLetter = woord.charAt(0);
                if(Character.isLetter(firstLetter)){
                    Text word = new Text();
                    word.set(Character.toString(firstLetter));
                    context.write(word, new IntWritable(1));
                }
            }
        }
    }
   
    // Reducer -> classes tussen <> zijn de classen van de (input key, input_value, output_key, output_value)
    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // key = hello, values = [1, 1, 1, 1]
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            System.out.println(sum);
            context.write(key, new IntWritable(sum));
        }
    }
    
    // configure the MapReduce program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "start letter java");
        job.setJarByClass(WordCount.class);
        // configure mapper
        job.setMapperClass(WCMapper.class);
        // configure combiner (soort van reducer die draait op mapping node voor performantie)
        job.setCombinerClass(WCReducer.class);
        // configure reducer
        job.setReducerClass(WCReducer.class);
        // set output key-value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set input file (first argument passed to the program)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set output file  (second argument passed to the program)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // In this case, we wait for completion to get the output/logs and not stop the program to early.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
