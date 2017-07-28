import java.awt.*;
import java.util.*;

public abstract class StudentDecorator extends Student {

    public Student decoratedStudent;

    public StudentDecorator(Student student) {
        super(student.name,student.position.getIntX(),student.position.getIntY(),student.data);
        this.decoratedStudent=student;

        data=decoratedStudent.data;
        name=decoratedStudent.name;
        position=decoratedStudent.position;
        state=decoratedStudent.state;
        id=decoratedStudent.id;
        number_of_courses_taken=decoratedStudent.number_of_courses_taken;
        tokens_currently_have=decoratedStudent.tokens_currently_have;

        hangi_derse_dogru=decoratedStudent.hangi_derse_dogru;
        color1=decoratedStudent.color1;
        for_rest_limit= decoratedStudent.for_rest_limit;
        for_rest=decoratedStudent.for_rest;
        hangi_derse_kac_basti = decoratedStudent.hangi_derse_kac_basti;
        ilkders=decoratedStudent.ilkders;
        speedd= decoratedStudent.speedd;

    }

    public abstract void draw(Graphics2D g2d);

    public abstract void act(double deltaTime,Data data);

    public abstract void act(double deltaTime);

}