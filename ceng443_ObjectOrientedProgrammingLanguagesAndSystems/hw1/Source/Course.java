import java.awt.*;

public class Course implements AnimationEntity {

    public Course(String namee, int minTokenss, int initialStudents, Color col,int xx, int yy, int widthh, int heightt) {

        name=namee;     color=col;      minTokens=minTokenss;       currently_enrolled_number=initialStudents;
        x=xx;           y=yy;           width=widthh;               height=heightt;
    }
    public int x,y,width,height,currently_enrolled_number,minTokens;

    public String name;

    public Color color;

    public void draw(Graphics2D g2d) {
        g2d.setPaint(color);
        g2d.fillRect(x,y,width,height);//x,y,with,height
        g2d.setPaint(Color.BLACK);
        g2d.drawString(name,x+5,y+15);
        g2d.drawString("Min Tokens : "+ Integer.toString(minTokens)  ,x+5,y+45);
        g2d.drawString("Students : "+ Integer.toString(currently_enrolled_number)  ,x+5,y+75);
    }
    public void act(double deltaTime) {}
}