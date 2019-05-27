import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Data {

    public Data() {
        windowsHeight=650;      windowsWidth=1000;

        fairy=new TokenFairy(650,1000,this);

        courses=new ArrayList<Course>();        tokens=new ArrayList<Token>();      students=new ArrayList<Student>();

        factory=new ArrayList<TokenFactory>();

        factory.add(new RegularFactory());      factory.add(new BlackFactory());    factory.add(new FakeFactory());

        int w=windowsWidth/8;

        Course ceng443=new Course("CENG443",80,0, Color.RED,0,0,w,100);
        Course ceng451=new Course("CENG451",20,0, Color.BLUE,w,0,w,100);
        Course ceng462=new Course("CENG462",10,0, Color.GREEN,2*w,0,w,100);
        Course ceng476=new Course("CENG476",10,0, Color.MAGENTA,3*w,0,w,100);
        Course ceng478=new Course("CENG478",10,0, Color.ORANGE,4*w,0,w,100);
        Course ceng495=new Course("CENG495",33,0, Color.CYAN,5*w,0,w,100);
        Course ceng497=new Course("CENG497",10,0, Color.PINK,6*w,0,w,100);
        Course ceng499=new Course("CENG499",55,0, Color.GRAY,7*w,0,w,100);

        courses.add(ceng443);
        courses.add(ceng451);
        courses.add(ceng462);
        courses.add(ceng476);
        courses.add(ceng478);
        courses.add(ceng495);
        courses.add(ceng497);
        courses.add(ceng499);

        for(int i=0; i<30; i++) // maximum 30 tokens can be
        {
            tokens.add(fairy.createToken());
            fairy.act(50L);
        }
        //String namee,int inx, int iny,
        students.add(new BasicStudent("Batyr",10,100+10,this));
        students.add(new BasicStudent("KadirCenk",50,100+40,this));
        students.add(new BasicStudent("BurakAkar",90,100+80,this));
        students.add(new BasicStudent("BerkayAydemir",150,100+120,this));
        students.add(new BasicStudent("Sercan",300,100+120,this));
        students.add(new BasicStudent("BurakKaan",235,100+120,this));
        students.add(new BasicStudent("MustafaAgirman",135,100+220,this));
        students.add(new BasicStudent("FiratCekinel",35,100+220,this));
        students.add(new BasicStudent("BugraDemirdoven",235,100+20,this));
        students.add(new BasicStudent("OnurHenden",35,100+120,this));


        students.add(new BasicStudent("BaskinBurak",35,100+120,this));
        students.add(new BasicStudent("ErincArgimak",45,100+130,this));
        students.add(new BasicStudent("BernaKabadayi",55,100+140,this));
        students.add(new BasicStudent("Melisa",65,100+150,this));
        students.add(new BasicStudent("Alara",75,100+160,this));
        students.add(new BasicStudent("Ozgu",85,100+170,this));
        students.add(new BasicStudent("CansuAlptekin",105,100+190,this));
        students.add(new BasicStudent("PinarBil",115,100+200,this));
        students.add(new BasicStudent("AliYilmaz",135,100+220,this));
        students.add(new BasicStudent("Afsin",145,100+230,this));

    }

    public void stepAll()
    {
        fairy.counter++;
        if(tokens.size()<10)
        {
            fairy.act(50L);
        }
        else if(fairy.counter==20)
        {
            fairy.counter=0;
            fairy.act(50L);
        }
        ///////////////////////////////////////////

        for(int i=0; i<students.size(); i++)
        {
            students.get(i).act(50L);
            if(students.get(i).number_of_courses_taken==1 && !students.get(i).flag)
            {
                students.get(i).color1=courses.get(students.get(i).hangi_derse_dogru-1).color;
                Student s=students.get(i);
                students.set(i,new Taken1(s));
                students.get(i).flag=true;
            }
            if(students.get(i).number_of_courses_taken==2 && !students.get(i).flag2)
            {

                students.get(i).color2=courses.get(students.get(i).hangi_derse_dogru-1).color;
                Student s=students.get(i);
                students.set(i,new Taken2(s));
                students.get(i).flag2=true;
            }
        }
        /////////////////////////
    }

    public List<TokenFactory> factory;

    public int windowsWidth;

    public int windowsHeight;

    public TokenFairy fairy;

    public List<Token> tokens;

    public List<Course> courses;

    public List<Student> students;

}