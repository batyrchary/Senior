import java.awt.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Student implements AnimationEntity {

    public Student(String namee,int inx, int iny,  Data dataa)
    {
        name=namee;     position=new Vector2D(inx,iny);     state=new Seek();
        id="seek";      number_of_courses_taken=0;          tokens_currently_have=0;
        data=dataa;     for_rest_limit= ThreadLocalRandom.current().nextInt(30, 50);
        for_rest=0;     hangi_derse_kac_basti = new int[]{0,0,0,0,0,0,0,0};
        ilkders=-1;     speedd= ThreadLocalRandom.current().nextInt(7, 10);
    }

    public Color color1,color2;
    public String name,id;

    public Vector2D position;

    public Integer speedd,number_of_courses_taken, tokens_currently_have, hangi_derse_dogru,for_rest,ilkders,for_rest_limit;

    public Data data;

    public State state, previous_state;

    public int[] hangi_derse_kac_basti;

    public boolean flag=false;
    public boolean flag2=false;

    public abstract void draw(Graphics2D g2d);
    public abstract void act(double deltaTime);
    public abstract void act(double deltaTime,Data data);

}