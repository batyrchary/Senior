import javax.swing.*;
import java.awt.*;
import java.util.*;

public class Display extends JPanel {

    public Display(Data data_from_dreamrunner) {
        super();
        setBackground(Color.WHITE);
        data=data_from_dreamrunner;
    }

    public Data data;

    public Dimension getPreferredSize() {
        return new Dimension(data.windowsWidth,data.windowsHeight);
    }

    public void paintComponent(Graphics g) {

        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;

        data.fairy.draw(g2d);

        for(int i=0; i<data.courses.size(); i++)
        {
            data.courses.get(i).draw(g2d);
        }

        for(int i=0; i<data.tokens.size(); i++)
        {
            data.tokens.get(i).draw(g2d);
        }

        for(int i=0; i<data.students.size(); i++)
        {
            data.students.get(i).draw(g2d);
        }
    }
}