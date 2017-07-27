import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.List;

public class Main extends JFrame{

    public static JButton Submit,Generate;
    public static JPanel mypanel;
    public static GridBagConstraints cc = new GridBagConstraints();
    public static Random random=new Random();


    public static JLabel attributteName,possible_values;
    public static JTextField lattributteName,lpossible_values,numbers;

    public static List<attribute> attributes=new ArrayList<>();
    public static List<String> a_names=new ArrayList<>();



    public static class attribute extends JPanel
    {
        public int scale=0;
        public GridBagConstraints cco = new GridBagConstraints();
        public  String name,value;
        public List<String> values=new ArrayList<>();

        public attribute(String namee, String valuee)
        {
            name=namee;
            value=valuee;

            String[] tokens;
            tokens= value.split("\\s*,\\s*");
            for(int i=0; i<tokens.length; i++)
                values.add(tokens[i]);

            setLayout(new GridBagLayout());
            cco.gridx = 0;cco.gridy = 0;
            add(new JLabel(name+": ["+valuee+"]"),cco);
            cco.gridx = 1;cco.gridy = 0;
            add(getSlider());
        }

        public String attribute_returner()
        {

            int r=random.nextInt(values.size());

            int sometimes_do_nothing=random.nextInt(7);

            if(sometimes_do_nothing!=1)
            {
                if (scale > 5 && scale <= 7) {
                    double arttir = values.size() / 4;
                    r = (int) Math.ceil(arttir + r);
                } else if (scale > 7 && scale <= 10) {
                    double arttir = values.size() / 2;
                    r = (int) Math.ceil(arttir + r);
                } else if (scale < 5 && scale >= 3) {
                    double arttir = values.size() / 4;
                    r = (int) Math.floor(r - arttir);
                } else if (scale < 3 && scale > 5) {
                    double arttir = values.size() / 2;
                    r = (int) Math.floor(r - arttir);
                }

                if (r >= values.size())
                    r = values.size() - 1;

                if (r < 0)
                    r = 0;
            }

            String ret=values.get(r);

            return ret;
        }
        public JSlider getSlider() {
            int min = 0, max = 10;

            JSlider slider= new JSlider(min,max,10);

            slider.setMajorTickSpacing(5);
            slider.setMinorTickSpacing(1);
            slider.setPaintTicks(true);
            slider.setValue(5);

            slider.addChangeListener(new ChangeListener() {
                public void stateChanged(ChangeEvent e) {
                    scale=slider.getValue();
                    //System.out.println(scale);
                }
            });
            return slider;
        }
    }



    public static void writer(int train, int test)
    {
        try{
            //PrintWriter writer = new PrintWriter("./myInput", "UTF-8");
            PrintWriter writer = new PrintWriter("./myInput", "UTF-8");

            writer.println("% Auto-generated on unknown date");
            writer.println();

            writer.println("%  'T:' Identifies the Attribute Title Line");
            writer.println("%  'A:' Identifies Training Set Examples");
            writer.println("%  'B:' Identifies Test Set Examples");
            writer.println();

            writer.print("T: ");
            for(int i=0; i<a_names.size(); i++)
            {
                    writer.print(a_names.get(i)+", ");
            }

            writer.println("result;");
            for(int i=0; i<train; i++)
            {
                writer.print("A: ");

                for(int j=0; j<a_names.size(); j++)
                {
                        writer.print(attributes.get(j).attribute_returner()+", ");
                }
                writer.println("yes;");
            }
            for(int i=0; i<test; i++)
            {
                writer.print("B: ");

                for(int j=0; j<a_names.size(); j++)
                {
                        writer.print(attributes.get(j).attribute_returner()+", ");
                }
                writer.println("yes;");
            }

            writer.close();
        } catch (IOException e) {
            // do something
        }
    }

    public static JButton getGenerate() {
        Generate = new JButton();
        Generate.setText("Generate");
        Generate.addActionListener(new java.awt.event.ActionListener() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                if(!numbers.getText().equals("") && !numbers.getText().equals("train_number, test_number"))
                {
                    try {
                        String[] tokens;
                        tokens = numbers.getText().split("\\s*,\\s*");

                        int train = Integer.parseInt(tokens[0]);
                        int test = Integer.parseInt(tokens[1]);
                        writer(train,test);
                    }
                    catch (NumberFormatException e)
                    {

                    }
                }
                numbers.setText("");

            }

        });
        return Generate;
    }

    public static JButton getSubmit() {
        Submit = new JButton();
        Submit.setText("Submit");
        Submit.addActionListener(new java.awt.event.ActionListener() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent evt) {

                if(!lattributteName.getText().equals("") && !lpossible_values.getText().equals(""))
                {
                    attribute myatt=new attribute(lattributteName.getText(),lpossible_values.getText());
                    a_names.add(lattributteName.getText());

                    attributes.add(myatt);
                    cc.gridy = cc.gridy+1;
                    mypanel.add(myatt,cc);
                    mypanel.updateUI();
                }

                lattributteName.setText("");
                lpossible_values.setText("");
            }

        });
        return Submit;
    }

    public Main()
    {
        super();
        setTitle("e2001527");
        setPreferredSize(new Dimension(850,700));
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);


        attributteName = new JLabel("attributte_name");
        lattributteName = new JTextField(20);
        possible_values =new JLabel("possible values");
        lpossible_values=new JTextField("separated by comma",20);

        mypanel = new JPanel();

        mypanel.setLayout(new GridBagLayout());

        cc.gridx = 0;cc.gridy = 0;
        mypanel.add(attributteName,cc);
        cc.gridx = 1;cc.gridy = 0;
        mypanel.add(lattributteName,cc);
        cc.gridx = 0;cc.gridy = 1;
        mypanel.add(possible_values,cc);
        cc.gridx = 1;cc.gridy = 1;
        mypanel.add(lpossible_values,cc);

        cc.gridx = 0;cc.gridy = 2;
        mypanel.add(getSubmit(),cc);

        cc.gridx = 0;cc.gridy = 3;
        mypanel.add(new JLabel("train,test:"),cc);
        cc.gridx = 1;cc.gridy = 3;
        numbers = new JTextField("train_number, test_number",20);
        mypanel.add(numbers,cc);

        cc.gridx = 0;cc.gridy = 4;
        mypanel.add(getGenerate(),cc);

        add(mypanel,BorderLayout.NORTH);
        pack();
    }



    public static void main(String[] args) {

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new Main().setVisible(true);
            }
        });
    }
}
