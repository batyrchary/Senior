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




public class Question5 extends Configured implements Tool 
{
	
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> 
    {

        private final static IntWritable one = new IntWritable(1);
       // public Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
           // String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) 
            {         
                String token = tokenizer.nextToken();   
                if(token.startsWith("#"))
                {
                    context.write(new Text(token),one);
                   // word.set(token.toLowerCase());
                   // context.write(word, one);
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
    {   
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
           // String line = value.toString();
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
           // String line = value.toString();
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

            int rows = 0;
            while (context.nextKeyValue()) 
            {
                
                if (rows++ == 100)
                    break;
            
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
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> 
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> trends, Context context) throws IOException, InterruptedException 
        {

            for (Text val : trends) 
            {
                context.write(new Text(val.toString()),new Text(key.toString()));
            }
        }
    }

   



	private static final String inputPath = "/user/hadoop1/tweets";
	private static final String outputPath ="/user/hadoop1/newtweetout";
	private static final String tempPath1 = "/user/hadoop1/newtweettemp1";
    private static final String tempPath2 = "/user/hadoop1/newtweettemp2";
	
    @Override
    public int run(String[] args) throws Exception 
    {
        Configuration conf = getConf();
        
        Job job1 = new Job(conf);
        job1.setJarByClass(Question5.class);       
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempPath1));

        boolean succ = job1.waitForCompletion(true);
        if (! succ) {
          System.out.println("Job1 failed, exiting");
          return -1;
        }
        
        
        //---------------------------------------------
        
        Job job2 = new Job(conf, "top-k-pass-2");
        FileInputFormat.setInputPaths(job2, new Path(tempPath1));
        FileOutputFormat.setOutputPath(job2, new Path(tempPath2));
       
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
       // job2.setNumReduceTasks(1);
        succ = job2.waitForCompletion(true);
        if (! succ) {
          System.out.println("Job2 failed, exiting");
          return -1;
        }


   //---------------------------------------------
        
        Job job3 = new Job(conf, "top-k-pass-3");
        FileInputFormat.setInputPaths(job3, new Path(tempPath2));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));
       
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer2.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
       // job3.setNumReduceTasks(1);
        succ = job3.waitForCompletion(true);
        if (! succ) {
          System.out.println("Job3 failed, exiting");
          return -1;
        }

        
        return 0;
        
    }

    public static void main(String[] args) throws Exception 
    {
    	String path [] = new String [4];
    	path[0] = inputPath;
    	path[1] = tempPath1;
        path[2] = tempPath2;
        path[3] = outputPath;
    	
        int res = ToolRunner.run(new Question5(), path);
        System.exit(res);
    }
}