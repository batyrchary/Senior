import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Question7 
{

	public static class UsersMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] splitted = value.toString().split("\\t");
			context.write(new Text(splitted[0]), new Text("user\t" + splitted[1].split(",")[0]));
		}
	}

	public static class TweetsMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();

			try
			{
				String[] splitted = line.split("\\t");

				if(isInteger(splitted[0]))
					context.write(new Text(splitted[0]), new Text("tweet\t" + splitted[1]));
			}
			catch(Exception ex)
			{}
		}

		boolean isInteger(String s){
			boolean val =false;
			try{
				int i=Integer.parseInt(s);
				val=true;
			}catch(Exception ex){
				val=false;
			}
			return val;
		}
	}

	public static class ReducerForJoin extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String city = "";
			int counter = 0;

			for (Text t : values) 
			{
				String splitted[] = t.toString().split("\\t");
		
				if (splitted[0].equals("tweet")) 
				{
					counter++;
				} 
				else if (splitted[0].equals("user"))
				{
					city = splitted[1];
				}
			}
			String str= String.valueOf(counter);
			context.write(new Text(city), new Text(str));
		}
	}

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
      
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString();
           
           	String[] splitted = line.split("\\t");
	        context.write(new Text(splitted[0]), new IntWritable(Integer.parseInt(splitted[1])));
        }
    }


    public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
    {   
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
            String line = value.toString();
            
			String[] splitted = line.split("\\t");
			context.write(new LongWritable(Long.parseLong(splitted[1])), new Text(splitted[0]));
        }
    }


    public static class Mapper3 extends Mapper<LongWritable, Text, LongWritable, Text>
    {    
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
            String line = value.toString();
            
			String[] splitted = line.split("\\t");
			context.write(new LongWritable(Long.parseLong(splitted[1])), new Text(splitted[0]));
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException 
        {
            setup(context);

            int counter = 0;
            while (context.nextKeyValue()) 
            {
                
                if (counter == 10)
                    break;
                counter++;

                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
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


	public static void main(String[] args) throws Exception 
	{

        if(args.length!=3)
        {
            System.out.println("Provide two input and output as command line argument");
            System.exit(1);
        }

        String inputPath1 = args[0];
        String inputPath2 = args[1];
        String outputPath4 = args[2];

        String outputPath1 = args[2]+"TempFile1";
        String outputPath2 = args[2]+"TempFile2";
        String outputPath3 = args[2]+"TempFile3";


		Configuration conf = new Configuration();

		Job job1 = new Job(conf, "Job1");
		job1.setJarByClass(Question7.class);


		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job1, new Path(inputPath1),TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputPath2),TextInputFormat.class, TweetsMapper.class);
		job1.setReducerClass(ReducerForJoin.class);
		
		
		FileOutputFormat.setOutputPath(job1, new Path(outputPath1));
		job1.waitForCompletion(true);
        

        /////////////////////////////////////////////////

        Job job2 = new Job(conf, "Job2");
        job2.setJarByClass(Question7.class);       
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        job2.setMapperClass(Mapper1.class);
        job2.setCombinerClass(Reducer1.class);
        job2.setReducerClass(Reducer1.class);
        

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job2, new Path(outputPath1));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));

        job2.waitForCompletion(true);
        
		/////////////////////////////////////////

 		Job job3 = new Job(conf, "Job3");
        job3.setJarByClass(Question7.class);

        FileInputFormat.setInputPaths(job3, new Path(outputPath2));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath3));
       
        job3.setMapperClass(Mapper2.class);
        job3.setReducerClass(Reducer2.class);
        
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
       
        job3.waitForCompletion(true);
        

        /////////////////////////////////////////////////
        
        Job job4 = new Job(conf, "Job4");
        job4.setJarByClass(Question7.class);

        FileInputFormat.setInputPaths(job4, new Path(outputPath3));
        FileOutputFormat.setOutputPath(job4, new Path(outputPath4));
       
        job4.setMapperClass(Mapper3.class);
        job4.setReducerClass(Reducer2.class);

        job4.setInputFormatClass(TextInputFormat.class);
        job4.setMapOutputKeyClass(LongWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        job4.waitForCompletion(true);
   

	}
}
