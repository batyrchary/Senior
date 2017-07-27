/**
 * Created by batyr on 15.04.2017.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import Jama.Matrix;

public class machine1 {

    private static String FILENAME ;

    public static ArrayList<ArrayList<String>> data = new ArrayList<>();
    public static double w[];
    public static double A[][]; //attribute matrix
    public static double B[]; //label matrix
    public static double er;

    public static double errorcalculator()
    {
        int n=data.size()-1;
        double sum=0;

        for(int i=1;i<data.size();i++)
        {
            double b=Double.parseDouble(data.get(i).get(w.length));//label li aldik

            double inner_sum=0;

            for(int j=0; j<w.length; j++)
            {
                inner_sum=inner_sum+(w[j] * Double.parseDouble(data.get(i).get(j)));
            }

            sum=sum+(b-inner_sum)*(b-inner_sum);
        }
        return (Math.sqrt(sum/n));
    }

    public static void printer()
    {
            System.out.printf("%1$-20s %2$10s %3$1.6f%n", "w0 (1.0)", "=", w[0]);

            for(int i=1;i<w.length; i++)
            {
                System.out.printf("%1$-20s %2$10s %3$1.6f%n", "w"+i+" ("+  data.get(0).get(i) +")", "=",w[i]);
            }
            System.out.println();
            System.out.printf("RMS Error = %s%n", er);

        /*
            w0 (1.0)           = -3.593303
            w1 (Attendance)    = 0.097791
            w2 (HW1)           = 0.152562
            w3 (HW2)           = 0.081378
            w4 (HW3)           = 0.226198
            w5 (Midterm)       = 0.292062

            RMS Error = 18.25625281111227
*/
    }

    public static void main(String[] args) {
 
		if (args.length > 0)
		{
			FILENAME=args[0]; 
		}
    		else
		{
			System.out.println("No argument was provided");
			return;
		}
        reader();
        transfor_to_matrix();
        calculate_w();
        er=errorcalculator();
        printer();
    }

    public static void calculate_w() {
        Matrix a = new Matrix(A);
        Matrix b = new Matrix(B,1);
        Matrix wm=(a.inverse()).times(b.transpose());

        for(int wh=0;wh<wm.getColumnDimension();wh++)
        {
            for(int wr=0;wr<wm.getRowDimension();wr++)
            {
                w[wr]=wm.get(wr,wh);
            }
        }
    }

    public static void transfor_to_matrix() {
        A=new double[data.size()-1][data.get(0).size()-1]; // eksi bir cunku ilk descriptionli ve label lazim degil kisim lazim degil
        B=new double[data.size()-1];//ilk description kismi almayan
        w=new double[data.get(0).size()-1];//label kismi almayarin

        int bindex=0;
        for(int i=1; i<data.size(); i++)
        {
            for(int j=0; j<data.get(0).size(); j++)
            {
                if(j!=data.get(0).size()-1)
                {
                    A[i-1][j] = Double.parseDouble(data.get(i).get(j));
                }
                else
                {
                    B[bindex]=Double.parseDouble(data.get(i).get(j));
                    bindex++;
                }
            }
        }
    }

    public static void reader() {
        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {

                ArrayList<String> list = new ArrayList<String>(Arrays.asList(sCurrentLine.split(",")));

                ArrayList<String> list2 = new ArrayList<>();
                list2.add("1");
                for(int i=0;i<list.size(); i++)
                {
                    list2.add(list.get(i));
                }

                data.add(list2);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
