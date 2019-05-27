import javax.swing.*;
import java.awt.*;
import java.util.*;

public class Regular extends Token {

    public Regular(int countt, int inx, int iny) {
        super(countt, inx, iny);
        actual=countt;
        icon=new ImageIcon("./fig/regular.png");
    }

    @Override
    public void draw(Graphics2D g2d) {
        super.draw(g2d);
    }
}