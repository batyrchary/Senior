/**
 * Created by batyr on 15.04.2017.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;

public class machine3 {

    private static String FILENAME;

    public static ArrayList<ArrayList<String>> data = new ArrayList<>();
    public static double w[]; //input
    public static int k;
    public static int KNindex[];


    public static double distance_calculator(int index)
    {
        double sum=0;
        for(int i=0; i<data.get(0).size()-1; i++)
        {
            sum=sum+Math.pow((w[i]-Double.parseDouble(data.get(index).get(i))),2);
        }
        double ret=Math.sqrt(sum);
        if(ret<0.001)
            return 0.001;

        return ret;
    }

    public static int find_max_index()
    {
        double max_dis=distance_calculator(KNindex[0]);
        int index_ret=0;
        for(int kindex=1; kindex<KNindex.length; kindex++)
        {
            if(max_dis<distance_calculator(KNindex[kindex]))
            {
                max_dis=distance_calculator(KNindex[kindex]);
                index_ret=kindex;
            }
        }

        return index_ret;
    }

    public static void function()
    {

        for(int index=k+1; index<data.size(); index++)
        {
            double distance=distance_calculator(index);

            int kindex=find_max_index();
            if(distance_calculator(KNindex[kindex])>distance)
            {
                KNindex[kindex]=index;
            }
        }
    }


    public static void sorter()
    {
        double[] array=new double[KNindex.length];
        double[] array2=new double[KNindex.length];
        int[] arrayindex=new int[KNindex.length];

        for(int kindex=0;kindex<KNindex.length; kindex++)
        {
            int index=KNindex[kindex];
            double dis=distance_calculator(index);

            array[kindex]=dis;
            array2[kindex]=dis;
            arrayindex[kindex]=index;
        }
        Arrays.sort( array );
//        System.out.println(Arrays.toString(array));

        for(int kindex=0;kindex<array.length; kindex++)
        {
            int index=0;
            for(int i=0; i<array2.length;i++)
            {
                if(array[kindex]==array2[i])
                {
                    index = i;
                    break;
                }
            }
            KNindex[kindex]=arrayindex[index];
        }
    }

    public static void printer()
    {
        double totalWeight=0;
        double totalWB=0;

        System.out.println("k="+k);
        System.out.println();

        for (int i = 0; i < w.length; i++) {
            System.out.printf("%1$-30s %2$10s %3$1.6f%n", "Attribute " + (i+1) + " (" + data.get(0).get(i) + ")", "=", w[i]);
        }
        System.out.println();
        for(int kindex=0;kindex<KNindex.length; kindex++)
        {
            System.out.print("Instance "+(kindex+1)+" : ");

            int index=KNindex[kindex];
            double dis=distance_calculator(index);
            System.out.print("Index = "+(index-1) +" , <");
            int i=0;
            for(; i<data.get(0).size()-1; i++)
            {
                System.out.print(Double.parseDouble(data.get(index).get(i))+", ");
            }
            System.out.print("> , Class Label = "+Double.parseDouble(data.get(index).get(i)));
            System.out.println(" , Distance = "+ dis +" , Weight = " + (1/dis));

            totalWB=totalWB+(1/dis)*Double.parseDouble(data.get(index).get(i));
            totalWeight=totalWeight+(1/dis);
        }

        System.out.println();
        System.out.println("Weighted class label (Final) = "+(totalWB/totalWeight));



/*
k = 5

Attribute 1 (Attendance)    = 91.000000
Attribute 2 (HW1)           = 92.000000
Attribute 3 (HW2)           = 93.000000
Attribute 4 (HW3)           = 94.000000
Attribute 5 (Midterm)       = 95.000000

Instance 1 : Index = 67 , < 92.0, 96.0, 85.0, 98.0, 95.0 > , Class Label = 82.0 , Distance = 9.848857801796104 , Weight = 0.10153461651336192
Instance 2 : Index = 9 , < 90.0, 91.0, 102.0, 95.0, 91.0 > , Class Label = 93.0 , Distance = 10.0 , Weight = 0.1
Instance 3 : Index = 172 , < 94.0, 94.0, 96.0, 100.0, 85.0 > , Class Label = 72.0 , Distance = 12.569805089976535 , Weight = 0.079555728417573
Instance 4 : Index = 249 , < 87.0, 98.0, 102.0, 99.0, 90.0 > , Class Label = 63.0 , Distance = 13.527749258468683 , Weight = 0.07392212709545729
Instance 5 : Index = 6 , < 87.0, 99.0, 103.0, 93.0, 101.0 > , Class Label = 94.0 , Distance = 14.212670403551895 , Weight = 0.07035975447302918

Weighted class label (Final) = 81.40

*/

    }

    public static void main(String[] args) {

		if(args.length<3)
		{
			System.out.println("incorrect argument was provided");
			return;
		}

        reader(args);
        function();
        sorter();
        printer();

    }

    public static void reader(String[] args) {

		if (args.length >=3 )
		{
			FILENAME=args[0];
			k=Integer.parseInt(args[1]);

			KNindex=new int[k];
			for(int i=0; i<k; i++)
			{		
			     KNindex[i]=i+1;
			}
		}
    		else
		{
			System.out.println("Incorrect argument was provided");
			return;
		}

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {

                ArrayList<String> list = new ArrayList<String>(Arrays.asList(sCurrentLine.split(",")));

                data.add(list);
            }

 	       //////////////initialize w
     	  w=new double[data.get(0).size()-1]; //size-1 attribute and 1 label
		  for(int i=0; i<w.length; i++)
		  {
	       	 w[i]=Double.parseDouble(args[i+2]);
	    	  }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

