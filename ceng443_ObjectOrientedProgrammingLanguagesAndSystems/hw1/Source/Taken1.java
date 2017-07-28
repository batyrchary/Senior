import java.awt.*;

public class Taken1 extends StudentDecorator {

    public Taken1(Student decoratedStudent1)
    {
        super(decoratedStudent1);
    }

    @Override
    public void draw(Graphics2D g2d) {

        data=decoratedStudent.data;
        name=decoratedStudent.name;
        position=decoratedStudent.position;
        state=decoratedStudent.state;
        id=decoratedStudent.id;
        number_of_courses_taken=decoratedStudent.number_of_courses_taken;
        tokens_currently_have=decoratedStudent.tokens_currently_have;
        color1=decoratedStudent.color1; color2=decoratedStudent.color2;
        hangi_derse_dogru=decoratedStudent.hangi_derse_dogru;
        for_rest_limit= decoratedStudent.for_rest_limit;
        for_rest=decoratedStudent.for_rest;
        hangi_derse_kac_basti = decoratedStudent.hangi_derse_kac_basti;
        ilkders=decoratedStudent.ilkders;
        speedd= decoratedStudent.speedd;

        g2d.setPaint(color1);
        g2d.fillRect(position.x+10,position.y+10,10,10);
        g2d.setPaint(Color.BLACK);
        decoratedStudent.draw(g2d);

    }

    @Override
    public void act(double deltaTime,Data data) {
        decoratedStudent.act(50L,data);
    }

    @Override
    public void act(double deltaTime) {
        act(50L,data);
    }
}