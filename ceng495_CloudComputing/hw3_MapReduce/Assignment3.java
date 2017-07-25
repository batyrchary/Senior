import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

public class Assignment3 {

	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				Text word = new Text();		//
				word.set(itr.nextToken());	
				context.write(word, new IntWritable(1));	//

			}
		}
	}

	public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			IntWritable result = new IntWritable(0);	//
			for (IntWritable val : values) 
			{
				result.set(result.get() + val.get());	//
			}
			context.write(key, result);
		}
	}

	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				Text word = new Text();//

				word.set(itr.nextToken());
				context.write(new Text("1"), new IntWritable(Integer.parseInt(word.toString())));

			}
		}
	}
		
	public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable evenMax = new IntWritable();
		private IntWritable oddMax = new IntWritable();
			
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int evenMax2=-1;
			int oddMax2=-1;
			for(IntWritable val : values)
			{
				if(evenMax2 < val.get() && val.get()%2==0)
				{
					evenMax2=val.get();
				}
				else if(oddMax2 < val.get() && val.get()%2==1)
				{
					oddMax2=val.get();
				}	
			}
			evenMax.set(evenMax2);
			oddMax.set(oddMax2);
			
				context.write(new Text("Even"), evenMax);
				context.write(new Text("Odd"), oddMax);
		}
	}	
	
	
	public static class Mapper3 extends Mapper<Object, Text, Text, IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				Text word = new Text(); //
				word.set(itr.nextToken());
				context.write(new Text("1"), new IntWritable(Integer.parseInt(word.toString())));

			}
		}
	}
		
	public static class Reducer3 extends Reducer<Text,IntWritable,Text,FloatWritable>
	{
		private FloatWritable evenAvg = new FloatWritable();
		private FloatWritable oddAvg = new FloatWritable();
			
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			float evenSum2=0;
			float oddSum2=0;
			Integer o=0;
			Integer e=0;
			for(IntWritable val : values)
			{
				if(val.get()%2==0)
				{
					e=e+1;
					evenSum2=evenSum2+val.get();
				}
				else if(val.get()%2==1)
				{
					o=o+1;
					oddSum2=oddSum2+val.get();
				}	
			}

			if(e!=0)
			{
				evenAvg.set(evenSum2/e);
				context.write(new Text("Even"), evenAvg);
			}
			if(o!=0)
			{
				oddAvg.set(oddSum2/o);
				context.write(new Text("Odd"), oddAvg);
			}
		}
	}	
	
	public static void main(String[] args) throws Exception {
	    
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Assignment3");
			job.setJarByClass(Assignment3.class);
			
			if(args[0].equals("count"))
			{
				job.setMapperClass(Mapper1.class);
				job.setCombinerClass(Reducer1.class);
				job.setReducerClass(Reducer1.class);
			}
			else if(args[0].equals("max"))
			{
				job.setMapperClass(Mapper2.class);
				job.setReducerClass(Reducer2.class);
			}
			else
			{
				job.setMapperClass(Mapper3.class);
				job.setReducerClass(Reducer3.class);
			}
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
