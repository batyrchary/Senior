import javax.swing.*;
import java.awt.*;

public class BasicStudent extends Student {

    public BasicStudent(String namee,int inx, int iny,  Data dataa) {
        super(namee,inx,iny,dataa);
    }

    @Override
    public void draw(Graphics2D g2d) {

        ImageIcon icon = new ImageIcon("./fig/student.png");
        icon.paintIcon(null, g2d,position.getIntX(),position.getIntY());

        g2d.drawString((name+" ("+speedd+")"),position.getIntX(),position.getIntY());
        
        if(id.equals("deposit"))
        {
        	g2d.setPaint(data.courses.get(hangi_derse_dogru-1).color);
        }
        g2d.drawString(id,position.getIntX()+7,position.getIntY()+40);
        g2d.setPaint(Color.BLACK);
        g2d.drawString(Integer.toString(number_of_courses_taken),position.getIntX()+20,position.getIntY()+25);
        g2d.drawString(Integer.toString(tokens_currently_have),position.getIntX()+20,position.getIntY()+55);
    }

    @Override
    public void act(double deltaTime)
    {
        act(deltaTime,data);
    }

    @Override
    public void act(double deltaTime,Data data) {
        state.act(this, deltaTime, data);
    }
}