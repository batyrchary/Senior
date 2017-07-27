import Jama.EigenvalueDecomposition;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import Jama.Matrix;
import Jama.Matrix.*;

import javax.swing.*;

public class Main {

    public static List<List<Float>> data = new ArrayList<List<Float>>();
    public static List<Float> means;
    public static int number_of_attributes,number_of_samples;
    public static double[][] matrix; // covariance matrix
    public static EigenvalueDecomposition eigenvalues;
    public static Matrix eigenvector;
    public static List<Integer> top_indexes=new ArrayList<>();

    public static Matrix final_result;
    public static int getNumber_of_attributes_reduced;



    public static void reader(String loc)
    {
        File file = new File("./"+loc);
        BufferedReader read = null;

        try {
            read = new BufferedReader(new FileReader(file));
            String text = null;

            int counter=0;
            while ((text = read.readLine()) != null) {

                String[] tokens;
                if(counter==2)
                {
                    List<Float> row=new ArrayList<Float>();
                    tokens= text.split("\\s*,\\s*");
                    for(int i=0; i<tokens.length; i++)
                        row.add(Float.parseFloat(tokens[i]));
                    data.add(row);
                }

                if(counter==1)
                    counter++;
                if(text.equals("@DATA"))
                    counter++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (read != null) {
                    read.close();
                }
            } catch (IOException e) {
            }
        }

    }

    public static void find_means()
    {
        means=new ArrayList<Float>();

        number_of_attributes=data.get(0).size();
        number_of_samples=data.size();
        for(int j=0; j<data.get(0).size(); j++) {
            float sum=0;
            for (int i = 0; i < data.size(); i++) {

                sum = sum + data.get(i).get(j);
            }
            means.add(sum/data.size());
        }
    }

    public static float covariance_calculator(int a1, int a2)
    {
        float sum=0;
        for(int i=0; i<data.size(); i++)
        {
            //(a1-mean'a1)*(a2-mean'a2)
            sum=sum+(data.get(i).get(a1)*means.get(a1) + data.get(i).get(a2)*means.get(a2));
        }
        return sum/number_of_samples;
    }

    public static void covariance_matrix()
    {
        matrix=new double[number_of_attributes][number_of_attributes];

        for(int i=0; i<number_of_attributes; i++)
        {
            for(int j=0; j<number_of_attributes; j++)
            {
                matrix[i][j]=covariance_calculator(i,j);
            }
        }
    }

    public static void eigenvalue_eigenvector_finder()
    {
        Matrix m=new Matrix(matrix);
        eigenvalues = m.eig();
        eigenvector=eigenvalues.getV();

    /*    for(int i=0; i<eigenvalues.getRealEigenvalues().length; i++)
            System.out.print(eigenvalues.getRealEigenvalues()[i]+", ");
        System.out.println();
        System.out.println("---------------------------");


        for (int i=0; i<eigenvector.getColumnDimension(); i++)
        {
            for(int j=0; j<eigenvector.getRowDimension(); j++)
            {
                System.out.print(eigenvector.get(i,j)+", ");
            }
            System.out.println();
        }
*/

    }

    public static void srter_and_picker(int reducenumber) //srter cause sout is important )
    {
        for(int i=0; i<reducenumber; i++)
        {
//            double min=Double.MIN_VALUE; //nedense altdakide dogru result basyo cok ilginc
            double min = -1;

            int index=0;
            for (int j = 0; j < eigenvalues.getRealEigenvalues().length; j++)
            {
                if(min<eigenvalues.getRealEigenvalues()[j] && !top_indexes.contains(j))
                {
                    index=j;
                }
            }
            top_indexes.add(index);
        }

    /*    System.out.println("<<<<<<<<<<>>>>>>>>>>>>>>>>>>");

        for (int i=0; i<top_indexes.size(); i++)
        {
            System.out.print(top_indexes.get(i)+", ");
        }
        System.out.println();
*/
    }


    public static void createAmatrix_and_multiply()
    {
        double A_matrix[][]=new double[top_indexes.size()][number_of_attributes];
        for(int i=0; i<top_indexes.size(); i++)
        {
            for(int j=0; j<eigenvector.getRowDimension(); j++)
            {
                A_matrix[i][j]=eigenvector.get(top_indexes.get(i),j);
            }
        }
        ////////////////////////////////
    /*    System.out.println("----------------------------------");
        for (int i=0;i<A_matrix.length;i++)
        {
            for(int j=0; j<A_matrix[0].length; j++)
            {
                System.out.print(A_matrix[i][j]+", ");
            }
            System.out.println();
        }
      */  /////////////////////////////

        double data_matrix[][]=new double[data.size()][data.get(0).size()];

        for(int i=0; i<data.size(); i++)
        {
            for(int j=0; j<data.get(0).size(); j++)
            {
                data_matrix[i][j]=data.get(i).get(j);
            }
        }
        Matrix m1=new Matrix(A_matrix);
        Matrix m3=new Matrix(data_matrix);
        Matrix m2=m3.transpose();
        Matrix m4=m1.times(m2);
        final_result=m4.transpose();

    }

    public static void writer()
    {

        try{
            PrintWriter writer = new PrintWriter("./reduced.arff", "UTF-8");

            writer.println("@RELATION Points");
            writer.println();
            for(int i=0; i<getNumber_of_attributes_reduced; i++)
            {
                writer.println("@ATTRIBUTE x"+i+" NUMERIC");
            }
            writer.println();
            writer.println("@DATA");
            writer.println();

            String written="";

            for(int r=0; r<final_result.getRowDimension(); r++)
            {

                for(int c=0; c<final_result.getColumnDimension(); c++)
                {
                    if(c!=final_result.getColumnDimension()-1)
                    {
//                      System.out.print(final_result.get(r, c) + " , ");
                        written=final_result.get(r, c) + " , ";
                        writer.print(written);

                    }
                    else
                    {
                     //   System.out.print(final_result.get(r, c));
                        written=""+final_result.get(r, c);
                        writer.print(written);
                    }
                }
//                System.out.println();
                writer.println();
            }

            writer.close();
        } catch (IOException e) {
            // do something
        }






    }

    public static void main(String[] args) {

        getNumber_of_attributes_reduced=Integer.parseInt(args[1]);
        reader(args[0]);                                           //get from user the file
        find_means();
                                                            //shurda cikartma islemini yapcazmi bilemedim
        covariance_matrix();
        eigenvalue_eigenvector_finder();
        srter_and_picker(getNumber_of_attributes_reduced);                                //get from user number of attributes
        createAmatrix_and_multiply();
        writer();

    }
}
