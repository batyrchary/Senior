import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.mapred.JobConf;



public class Question6 
{
    
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> 
    {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) 
            {         
                String token = tokenizer.nextToken();   
                if(token.matches("(19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])"))
                {
                    context.write(new Text(token),new IntWritable(1));
                }
            }

        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
    {   
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) 
            {       
                String token = tokenizer.nextToken();   
                context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));         
            }
        }
    }


    public static class Mapper3 extends Mapper<LongWritable, Text, LongWritable, Text>
    {    
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) 
            {    
                String token = tokenizer.nextToken();   
                context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));         
            }
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException 
        {
            setup(context);

            int counter = 0;
            while (context.nextKeyValue()) 
            {
                
                if (counter == 5)
                    break;
                counter++;

                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
        }
    }


    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> 
    { 
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable v : values) 
            {
                sum = sum + v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> 
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {

            for (Text v : values) 
            {
                context.write(new Text(v.toString()),new Text(key.toString()));
            }
        }
    }


    public static void main(String[] args) throws Exception 
    {
        
        if(args.length!=2)
        {
            System.out.println("Provide input and output as command line argument");
            System.exit(1);
        }


        String inputPath = args[0];
        String outputPath = args[1];
        String tempPath1 = args[1]+"TempFile1";
        String tempPath2 = args[1]+"TempFile2";

        
        Configuration conf = new Configuration();
        
        Job job1 = new Job(conf);
        job1.setJarByClass(Question6.class);       


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempPath1));


        job1.waitForCompletion(true);
    
        
        ////////////////////////////////////////////////////
        Job job2 = new Job(conf, "Job2");
        job2.setJarByClass(Question6.class); 

        
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
       
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);  
      
        
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);        
       // job2.setNumReduceTasks(1);


        FileInputFormat.setInputPaths(job2, new Path(tempPath1));
        FileOutputFormat.setOutputPath(job2, new Path(tempPath2));

        job2.waitForCompletion(true);


        ////////////////////////////////////////////////////
   
        
        Job job3 = new Job(conf, "Job3");
        job3.setJarByClass(Question6.class); 


        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
       
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer2.class);


        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);


        job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        // job3.setNumReduceTasks(1);


        FileInputFormat.setInputPaths(job3, new Path(tempPath2));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));
       
        job3.waitForCompletion(true);
    }
}