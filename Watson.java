import java.io.BufferedReader;
import java.io.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Iterator;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;


public class Main {

            @SuppressWarnings("uncheked")
            public static void main(String args[]){

                JSONParser parser = new JSONParser();


                try {
                    Object obj = parser.parse(new FileReader("/Users/batyrchary/Desktop/A1BG.json"));


                    JSONObject jsonObject =  (JSONObject) obj;
                    JSONArray relations = (JSONArray) jsonObject.get("relations");


                    for (int i=0; i<relations.size(); i++)
                    {
                        JSONObject relation = (JSONObject) relations.get(i);

                        String type=(String)relation.get("type");
                        double score = ((Number)relation.get("score")).doubleValue();

                        JSONArray arguments = (JSONArray) relation.get("arguments");

                        JSONObject arg1=(JSONObject) arguments.get(0);
                        JSONObject arg2=(JSONObject) arguments.get(1);

                        String gene1=(String) arg1.get("text");
                        String gene2=(String) arg2.get("text");

                        System.out.println(gene1 +" "+ type+ " " + gene2 +"="+score);

                    }

                   // System.out.println(relations.size());


                } catch (Exception e)
                {
                    e.printStackTrace();
                }

            }
        }




