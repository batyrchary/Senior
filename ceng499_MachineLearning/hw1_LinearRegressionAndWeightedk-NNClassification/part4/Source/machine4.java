/**
 * Created by batyr on 15.04.2017.
 */
import Jama.Matrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;


public class machine4 {

    private static String FILENAME;
    private static String FILENAME2;

    public static ArrayList<ArrayList<String>> data = new ArrayList<>();
    public static double[] price;
    public static double[][] table_according_to_window;
    public static int window;
    public static double stoppingcondition;
    public static double stepsize;

    public static ArrayList<ArrayList<String>> data2 = new ArrayList<>();
    public static double[] price2;
    public static double[][] table_according_to_window2;
    public static double A2[][]; //attribute matrix
    public static double B2[]; //label matrix

    public static double A[][]; //attribute matrix
    public static double B[]; //label matrix
    public static double w1[];
    public static double w2[]; //initially all zero
    public static double wreserve[]; //initially all zero


    public static void main(String[] args) {
	   
	   	if(args.length<5)
		{
			System.out.println("incorrect argument was provided");
			return;
		}
		else
		{
			window=Integer.parseInt(args[0]);
			FILENAME=args[1];
			FILENAME2=args[2];
			stepsize=Double.parseDouble(args[3]);
			stoppingcondition=Double.parseDouble(args[4]);	
		}

        reader();
        average_price();
        table_according_to_window_size();
        writer();
        calculate1_w();
        calculate2_w();
        printer();
    }

    public static double errorcalculator2(int flag)
    {
        int n = B2.length;
        double sum = 0;

        for (int i = 0; i < A2.length; i++) {
            double b = B2[i];//label li aldik

            double inner_sum = 0;

            if(flag==1) {
                for (int j = 0; j < w1.length; j++) {
                    inner_sum = inner_sum + (w1[j] * A[i][j]);
                }
            }
            else
            {
                for (int j = 0; j < w2.length; j++) {
                    inner_sum = inner_sum + (w2[j] * A[i][j]);
                }
            }
            sum = sum + (b - inner_sum) * (b - inner_sum);
        }
        return (Math.sqrt(sum / n));
    }


    public static void printer()
    {

        System.out.println("W matrix and RMS-Error from Part1");
        System.out.print("w = < ");
        for(int i=0; i<w1.length;i++)
        {
            if(i!=w1.length-1)
                System.out.print(w1[i]+", ");
            else
                System.out.println(w1[i]+" >");
        }
        double error1=errorcalculator(1);
        System.out.println("RMS-Error="+error1);
        System.out.println();
////////////////////////////////////

        System.out.println("W matrix and RMS-Error from Part2");
        System.out.print("w = < ");
        for(int i=0; i<w2.length;i++)
        {
            if(i!=w2.length-1)
                System.out.print(w2[i]+", ");
            else
                System.out.println(w2[i]+" >");
        }
        double error2=errorcalculator(2);
        System.out.println("RMS-Error="+error2);
        System.out.println();
////////////////////////////////////
        System.out.println("Predicted Error from 2017");
        System.out.println("Error Using Weight value from Part1="+errorcalculator2(1));
        System.out.println("Error Using Weight value from Part2="+errorcalculator2(2));
    }

    public static double errorcalculator(int flag)
    {
            int n = B.length;
            double sum = 0;

            for (int i = 0; i < A.length; i++) {
                double b = B[i];//label li aldik

                double inner_sum = 0;

                if(flag==1) {
                    for (int j = 0; j < w1.length; j++) {
                        inner_sum = inner_sum + (w1[j] * A[i][j]);
                    }
                }
                else
                {
                    for (int j = 0; j < w2.length; j++) {
                        inner_sum = inner_sum + (w2[j] * A[i][j]);
                    }
                }
                sum = sum + (b - inner_sum) * (b - inner_sum);
            }
            return (Math.sqrt(sum / n));
    }

    public static double derivative_calculator(int index) {
        int n = B.length;//ilk basda descri[tion bar ====> HW1,HW2,HW3,MID,FINAL
        double sum = 0;
        for (int i = 0; i < A.length; i++) {
            double b = B[i];//label li aldik
            double aj = A[i][index];

            double inner_sum = 0;

            for (int j = 0; j < w2.length; j++) {
                inner_sum = inner_sum + (w2[j] * A[i][j]);
            }

            sum = sum + (-aj * (b - inner_sum)); //// aj olcak ama anlayamadim onun ne oldugunu o yuzden biraktim
        }
        double derivative = (2 * sum) / n;

        return derivative;
    }

    public static void w_updater() {
		double initial_error = errorcalculator(2);
            ////manipulate w
            for (int i = 0; i < w2.length; i++) {
                wreserve[i] = w2[i] - stepsize * derivative_calculator(i);
            }
            ///////////////
            double[] wreserve2=new double[w2.length];

            for (int i = 0; i < w2.length; i++) {
                wreserve2[i]=w2[i];
                w2[i] = wreserve[i];
            }
            if(String.valueOf(errorcalculator(2))=="Infinity")
            {
                for (int i = 0; i < w2.length; i++) {
                    w2[i] = wreserve2[i];
                }
                return;
            }
		  if (Math.abs(initial_error - errorcalculator(2)) < stoppingcondition ){
                return;
            }          


		  w_updater();
    }

    public static void calculate2_w()
    {
        w2 = new double[w1.length]; //size-1 attribute and 1 label
        wreserve = new double[w1.length]; //size-1 attribute and 1 label
        for (int i = 0; i < w2.length; i++) {
            w2[i] = 0;
            wreserve[i] = 0;
        }
        w_updater();
    }

    public static void calculate1_w()
    {
        A=new double[table_according_to_window.length][table_according_to_window[0].length];
        B=new double[table_according_to_window.length];

        for (int i=0; i<table_according_to_window.length; i++)
        {
            A[i][0]=1;
            for(int j=1; j<table_according_to_window[0].length; j++)
            {
              A[i][j]=table_according_to_window[i][j-1];
            }
            B[i]=table_according_to_window[i][table_according_to_window[0].length-1];
        }
        Matrix a = new Matrix(A);
        Matrix b = new Matrix(B,1);

        Matrix wm=(a.inverse()).times(b.transpose());

        w1=new double[table_according_to_window[0].length]; //cunku 0, hep 1

        for(int wh=0;wh<wm.getColumnDimension();wh++)
        {
            for(int wr=0;wr<wm.getRowDimension();wr++)
            {
                w1[wr]=wm.get(wr,wh);
            }
        }
        //////////////////////////////////

        A2=new double[table_according_to_window2.length][table_according_to_window2[0].length];
        B2=new double[table_according_to_window2.length];

        for (int i=0; i<table_according_to_window2.length; i++)
        {
            A2[i][0]=1;
            for(int j=1; j<table_according_to_window2[0].length; j++)
            {
                A2[i][j]=table_according_to_window2[i][j-1];
            }
            B2[i]=table_according_to_window2[i][table_according_to_window2[0].length-1];
        }

    }

    public static void table_according_to_window_size()
    {
        int h_table=price.length-window;
        table_according_to_window=new double[h_table][window+1];//6 gun ise 7nji gunem olcak label olarak

        for(int i=0; i<h_table; i++)
        {
            for(int j=0; j<window+1; j++)
            {
                table_according_to_window[i][j]=price[i+j];
            }
        }
        //////////////////////////////////////

        int h_table2=price2.length-window;
        table_according_to_window2=new double[h_table2][window+1];//6 gun ise 7nji gunem olcak label olarak

        for(int i=0; i<h_table2; i++)
        {
            for(int j=0; j<window+1; j++)
            {
                table_according_to_window2[i][j]=price2[i+j];
            }
        }

    }

    public static void writer()
    {

        try{
            PrintWriter writer = new PrintWriter("./Data1_out", "UTF-8");

            for(int i=0; i<window; i++)
            {
                if(i!=(window-1))
                    writer.print("price"+(i+1)+", ");
                else
                {
                    writer.println("price"+(i+1));
                }
            }

            for(int i=0; i<table_according_to_window.length; i++)
            {
                for(int j=0; j<table_according_to_window[0].length; j++)
                {
                    if(j!=table_according_to_window[0].length-1)
                        writer.print(table_according_to_window[i][j]+", ");
                    else
                        writer.print(table_according_to_window[i][j]);
                }
                writer.println();
            }
            writer.close();
        } catch (IOException e) {
            // do something
        }

        ////////////////////////////////

        try{
            PrintWriter writer2 = new PrintWriter("./Data2_out", "UTF-8");

            for(int i=0; i<window; i++)
            {
                if(i!=(window-1))
                    writer2.print("price"+(i+1)+", ");
                else
                {
                    writer2.println("price"+(i+1));
                }
            }

            for(int i=0; i<table_according_to_window2.length; i++)
            {
                for(int j=0; j<table_according_to_window2[0].length; j++)
                {
                    if(j!=table_according_to_window2[0].length-1)
                        writer2.print(table_according_to_window2[i][j]+", ");
                    else
                        writer2.print(table_according_to_window2[i][j]);
                }
                writer2.println();
            }
            writer2.close();
        } catch (IOException e) {
            // do something
        }







    }

    public static void average_price()
    {
        price=new double[data.size()-1];//description alinmadi
        int numberof_attributes=data.get(0).size()-3;//date almadim,label,ve volume almadim

        for(int i=1; i<data.size(); i++) //description almadim
        {
            double av=0;
            for(int j=1; j<data.get(0).size()-2; j++) //date almadim, label ve volume almadim
            {
                av=av+Double.parseDouble(data.get(i).get(j));
            }

            price[i-1]=av/numberof_attributes;
        }

        //////////////////////////////////////

        price2=new double[data2.size()-1];//description alinmadi
        int numberof_attributes2=data2.get(0).size()-3;//date almadim,label,ve volume almadim

        for(int i=1; i<data2.size(); i++) //description almadim
        {
            double av=0;
            for(int j=1; j<data2.get(0).size()-2; j++) //date almadim, label ve volume almadim
            {
                av=av+Double.parseDouble(data2.get(i).get(j));
            }

            price2[i-1]=av/numberof_attributes2;
        }
    }

    public static void reader() {
        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {

                ArrayList<String> list = new ArrayList<String>(Arrays.asList(sCurrentLine.split(",")));

                ArrayList<String> list2 = new ArrayList<>();
                for(int i=0;i<list.size(); i++)
                {
                    list2.add(list.get(i));
                }

                data.add(list2);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //////////////////////////////////////

        try (BufferedReader br2 = new BufferedReader(new FileReader(FILENAME2))) {

            String sCurrentLine2;

            while ((sCurrentLine2 = br2.readLine()) != null) {

                ArrayList<String> list3 = new ArrayList<String>(Arrays.asList(sCurrentLine2.split(",")));

                ArrayList<String> list23 = new ArrayList<>();
                for(int i=0;i<list3.size(); i++)
                {
                    list23.add(list3.get(i));
                }

                data2.add(list23);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
