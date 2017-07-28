import javax.swing.*;
import java.awt.*;
import java.util.*;

public class Fake extends Token {

    public Fake(int countt, int inx, int iny) {
        super(countt, inx, iny);
        actual=1;
        icon=new ImageIcon("./fake.jpg");
    }

    @Override
    public void draw(Graphics2D g2d) {
        super.draw(g2d);
    }
}