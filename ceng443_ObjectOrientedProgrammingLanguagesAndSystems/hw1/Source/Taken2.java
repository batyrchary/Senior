import java.awt.*;

public class Taken2 extends StudentDecorator {

    public Taken2(Student decoratedStudent2)
    {
        super(decoratedStudent2);
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
        data=decoratedStudent.data;
        for_rest_limit= decoratedStudent.for_rest_limit;
        for_rest=decoratedStudent.for_rest;
        hangi_derse_kac_basti = decoratedStudent.hangi_derse_kac_basti;
        ilkders=decoratedStudent.ilkders;
        speedd=decoratedStudent.speedd;

        decoratedStudent.color2=data.courses.get(decoratedStudent.hangi_derse_dogru-1).color;
        color2=decoratedStudent.color2;     color1=decoratedStudent.color1;

        g2d.setPaint(color2);
        g2d.fillRect(position.x+30,position.y+10,10,10);
        g2d.setPaint(Color.BLACK);
        decoratedStudent.draw(g2d);
    }
    @Override
    public void act(double deltaTime,Data data)
    {
        decoratedStudent.act(50L,data);
    }

    @Override
    public void act(double deltaTime)
    {
        act(50L,data);
    }
}