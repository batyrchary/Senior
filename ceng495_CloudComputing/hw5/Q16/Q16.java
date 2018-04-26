package hw2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

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



public class Q16 
{
 
    public static void main(String[] args) 
    {
        
    	if(args.length<1)
        {
            System.out.println("Provide at least input(path to training_set_tweets.txt) as command line argument");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("Q16")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        
        //String path = "/home/hadoop1/Desktop/EnginHW2/Q16/training_set_tweets.txt";
        String path = args[0];
        
        
        JavaRDD<String> lines = sc.textFile(path);	// read text file to RDD
        
////////////////////////////////////   
        //from each line finds mentions, puts into list, finally filters out distinct mentions
        JavaRDD<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) {
                    	
                    	Pattern pattern = Pattern.compile("@\\w+");

                    	java.util.regex.Matcher matcher = pattern.matcher(s);
                    	
                    	ArrayList<String> mylist= new ArrayList<String>();
                    	while (matcher.find())
                    	{
                    		mylist.add(matcher.group());
                    	}
                    	List<String> distinctList = mylist.stream().distinct().collect(Collectors.toList());
                    	return distinctList.iterator();
                    }
                });
////////////////////////////////////    
        
        
////////////////////////////////////
        //applying word_count
        JavaPairRDD<String, Integer> pairs = words.mapToPair(
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
        if(args.length==2)
        	swappedPairfinal.saveAsTextFile("./"+args[1]);
        else
        {
        	for(Tuple2<String, Integer> line:swappedPairfinal.take(20))
        	{
                System.out.println(line); 
        	}
      
        }
    }
}
    
    