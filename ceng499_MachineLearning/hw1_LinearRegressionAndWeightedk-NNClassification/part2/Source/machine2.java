/**
 * Created by batyr on 15.04.2017.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;

public class machine2 {

    private static String FILENAME;

    public static ArrayList<ArrayList<String>> data = new ArrayList<>();
    public static double w[]; //initially all zero
    public static double wreserve[]; //initially all zero
    public static double lasterror;
    public static double stoppingcondition;
    public static double mu;

    public static double derivative_calculator(int index) {
        int n = data.size() - 1;//ilk basda descri[tion bar ====> HW1,HW2,HW3,MID,FINAL
        double sum = 0;
        for (int i = 1; i < data.size(); i++) {
            double b = Double.parseDouble(data.get(i).get(w.length));//label li aldik
            double aj = Double.parseDouble(data.get(i).get(index));

            double inner_sum = 0;

            for (int j = 0; j < w.length; j++) {
                inner_sum = inner_sum + (w[j] * Double.parseDouble(data.get(i).get(j)));
            }

            sum = sum + (-aj * (b - inner_sum)); //// aj olcak ama anlayamadim onun ne oldugunu o yuzden biraktim
        }
        double derivative = (2 * sum) / n;

        return derivative;
    }

    public static double errorcalculator() {
        int n = data.size() - 1;
        double sum = 0;

        for (int i = 1; i < data.size(); i++) {
            double b = Double.parseDouble(data.get(i).get(w.length));//label li aldik

            double inner_sum = 0;

            for (int j = 0; j < w.length; j++) {
                inner_sum = inner_sum + (w[j] * Double.parseDouble(data.get(i).get(j)));
            }
            sum = sum + (b - inner_sum) * (b - inner_sum);
        }
        return (Math.sqrt(sum / n));
    }

    public static void printer() {
        System.out.print("< ");
        for (int i = 0; i < w.length; i++) {
            if (i == w.length - 1) {
                System.out.print(w[i] + " >");
            } else {
                System.out.print(w[i] + ", ");
            }
        }
        double er = errorcalculator();
        System.out.println(" , RMS Error = " + er);
    }

    public static void w_updater() {
        double initial_error = errorcalculator();
        printer();
        ////manipulate w
        for (int i = 0; i < w.length; i++) {
            wreserve[i] = w[i] - mu * derivative_calculator(i);
        }
        ///////////////
        double[] wreserve2=new double[w.length];

        for (int i = 0; i < w.length; i++) {
            wreserve2[i]=w[i];
            w[i] = wreserve[i];
        }
        if(String.valueOf(errorcalculator())=="Infinity")
        {
            for (int i = 0; i < w.length; i++) {
                w[i] = wreserve2[i];
            }
            printer();
            return;
        }



        if (Math.abs(initial_error - errorcalculator()) < stoppingcondition) {
            printer();
            return;
        }
        w_updater();
    }

    public static void lastprinter() {
        System.out.printf("%1$-20s %2$10s %3$1.6f%n", "w0 (1.0)", "=", w[0]);

        for (int i = 1; i < w.length; i++) {
            System.out.printf("%1$-20s %2$10s %3$1.6f%n", "w" + i + " (" + data.get(0).get(i) + ")", "=", w[i]);
        }
        System.out.println();
        System.out.printf("RMS Error = %s%n", lasterror);
    }

    public static void main(String[] args) {
 
		if (args.length == 3)
		{
			FILENAME=args[0];
			mu=Double.parseDouble(args[1]);
			stoppingcondition=Double.parseDouble(args[2]); 
		}
    		else
		{
			System.out.println("No argument was provided");
			return;
		}


        reader();
        w_updater();
        lasterror = errorcalculator();
        System.out.println();
        lastprinter();

    }

    public static void reader() {
        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {

                ArrayList<String> list = new ArrayList<String>(Arrays.asList(sCurrentLine.split(",")));

                ArrayList<String> list2 = new ArrayList<>();
                list2.add("1");
                for (int i = 0; i < list.size(); i++) {
                    list2.add(list.get(i));
                }

                data.add(list2);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        //////////////initialize w
        w = new double[data.get(0).size() - 1]; //size-1 attribute and 1 label
        wreserve = new double[data.get(0).size() - 1]; //size-1 attribute and 1 label
        for (int i = 0; i < w.length; i++) {
            w[i] = 0;
            wreserve[i] = 0;
        }
        //////////////////////
    }

}
