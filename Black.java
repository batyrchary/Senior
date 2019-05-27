import javax.swing.*;
import java.awt.*;
import java.util.*;

public class Black extends Token {

    public Black(int countt, int inx, int iny) {
        super(countt, inx, iny);
        actual=countt/2;
        icon=new ImageIcon("./fig/black.jpg");
    }

    @Override
    public void draw(Graphics2D g2d)
    {
        super.draw(g2d);
    }

}