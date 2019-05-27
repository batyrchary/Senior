import javax.swing.*;
import java.awt.*;
import java.util.*;

public class Token implements AnimationEntity {

    public Token(int countt, int inx, int iny) {
        count=countt;
        position=new Vector2D(inx,iny);
    }

    private int count;      public int actual;      public Vector2D position;       public ImageIcon icon;

    public void draw(Graphics2D g2d) {
        icon.paintIcon(null, g2d, position.getIntX(), position.getIntY());
        g2d.drawString(Integer.toString(count),position.getIntX(),position.getIntY());
    }

    public int getCount() {
        return actual;
    }
    public void act(double deltaTime) {}
}