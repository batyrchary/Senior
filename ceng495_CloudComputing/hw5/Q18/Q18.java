package hw2;


 
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.lang.Iterable;

import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import com.google.inject.matcher.Matcher;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.Optional;



public class Q18 
{
	  public static void main(String[] args) 
	  {
	        
		  	if(args.length<2)
	        {
	            System.out.println("Provide at least inputs(path to training_set_tweets.txt and training_set_users.txt) as command line argument");
	            System.exit(1);
	        }
		  
	        SparkConf sparkConf = new SparkConf().setAppName("Q18")
	                                        .setMaster("local[2]").set("spark.executor.memory","2g");
	        
	        JavaSparkContext sc = new JavaSparkContext(sparkConf);
	        
	        
//	        String path1 = "/home/hadoop1/Desktop/EnginHW2/Q16/training_set_tweets.txt";
//	        String path2 = "/home/hadoop1/Desktop/EnginHW2/Q16/training_set_users.txt";
	        String path1 = args[0];
	        String path2 = args[1];
	        
	        JavaRDD<String> twitters = sc.textFile(path1);
	        JavaRDD<String> users = sc.textFile(path2);
	        
	        
////////////////////////////////////
	        //filters date, I could first make join then filter but it kept giving me errors on parsing.
	        JavaRDD<String> twittersFiltered = twitters.filter(new Function<String, Boolean>() 
	        {
	        	@Override
	        	public Boolean call(String s) throws Exception 
	        	{	
	        		
		        	if(s==null)
		        		return false;
		        		
		        	String splitted[]=s.split("\t");
		        		
		        	if(splitted.length<3)
		        		return false;
		        		
		            String time=splitted[splitted.length-1];
		        		
		            //2009-10-15 13:50:14
		            String timesplitted[]=time.split(" ");
		            	
		            if(timesplitted.length<2)
		            	return false;
		            	
		            String datesplitted[]=timesplitted[0].split("-");
		        	
		            if(datesplitted.length<3)
		            	return false;
		            	
		            	
		            String year=datesplitted[0];
		            String month=datesplitted[1];
		           
		            int day=0;
		            	try {	
		            		day=(datesplitted[2].charAt(0)-'0')*10+(datesplitted[2].charAt(1)-'0');
		            	}
		            	catch(Exception e) {
		            	
		            	}
		            
		            if(!year.equals("2009") || !month.equals("09") || !(day>=16 && day<=20 ))
		            	return false;

	        		return true;
	        	}
	        		
	        }); 
////////////////////////////////////

////////////////////////////////////
	        // make Pair RDDs and rightJoin
	        JavaPairRDD<String, String> pairtwitters= twittersFiltered.mapToPair(new PairFunction<String, String, String>() 
	        {
	        	@Override
	        	public Tuple2<String, String> call(String str) throws Exception 
	        	{
	        		String[] data = str.split("\t", 2);
	        		return new Tuple2<String, String>(data[0], data[1]);	
	        	}
	        });
	        
	        JavaPairRDD<String, String> pairusers= users.mapToPair(new PairFunction<String, String, String>() 
	        {
	        	@Override
	        	public Tuple2<String, String> call(String str) throws Exception 
	        	{
	        		String[] data = str.split("\t", 2);	
	        		return new Tuple2<String, String>(data[0], data[1]);
	        	}
	        });
	        //RightJoin
	        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightJoinRdd=pairusers.rightOuterJoin(pairtwitters);
			 
			//make Join output more appropriate to deal with get rid of Optional
			JavaPairRDD<String, String> mappedRDD = rightJoinRdd.mapToPair(tuple -> 
			{
				return new Tuple2<String, String>("nothing",tuple._1+"\t"+tuple._2._1.orNull()+"\t"+tuple._2._2);
			   
			});
			
////////////////////////////////////
			
////////////////////////////////////
// Filter city get rid of nothing field
			JavaRDD<String> newRDD = mappedRDD.map(x -> x._2);
		
	        JavaRDD<String> filteredCity = newRDD.filter(new Function<String, Boolean>() 
	        {
	        	@Override
	        	public Boolean call(String s) throws Exception 
	        	{	
		        	String city=s.split("\t")[1];
		        	
		        	if(city.equals("Los Angeles, CA") || city.equals("Los Angeles") )
		        		return true;
		        	
	        		return false;
	        	}
	        }); 
////////////////////////////////////
			
////////////////////////////////////			
	       //take only IDS and apply word count
			JavaRDD<String> FittingUserIDs = filteredCity
		                .flatMap(new FlatMapFunction<String, String>() {
		                    public Iterator<String> call(String s) {
		  
		                    	String splitted[]=s.split("\t");
		                    	String userID=splitted[0];
		                    	
		                    	ArrayList<String> mylist= new ArrayList<String>();
		                    	
		                    	mylist.add(userID);
		                    
		                    	return mylist.iterator();
		                    }
		                });
			  
			  JavaPairRDD<String, Integer> pairs = FittingUserIDs.mapToPair(
		                new PairFunction<String, String, Integer>(){
		                    public Tuple2<String, Integer> call(String s){
		                            return new Tuple2(s, 1);
		                        }
		                } );
			
			  JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
		                new Function2<Integer, Integer, Integer>() {
		                    public Integer call(Integer a, Integer b) {
		                        return a + b;
		                    }
		                }, 1);
			  	
			  	//swap key-value, to sort by key
		        JavaPairRDD<Integer, String> swappedPair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
		            @Override
		            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
		                return item.swap();
		            }

		         });
		        
		        //sort by key
		        JavaPairRDD<Integer, String>  swappedPairSorted= swappedPair.sortByKey(false);
		        
		        //swap back key-value
		        JavaPairRDD<String, Integer> swappedPairfinal = swappedPairSorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
		            @Override
		            public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
		                return item.swap();
		            }

		         });
////////////////////////////////////
				
////////////////////////////////////
		       
		        
		        //print or write to outputfile
		        if(args.length==3)
		        {
		        	swappedPairfinal.saveAsTextFile("./"+args[2]);
		        }
		        else
		        {
		        	for(Tuple2<String, Integer> line:swappedPairfinal.take(10))
		        	{
		                System.out.println(line); 
		        	}
		        }
		        
	  }	
}
