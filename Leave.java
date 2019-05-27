import java.util.*;

public class Leave extends State {

    public Leave() {}

    @Override
    public void act(Student student, double deltaTime, Data data)
    {
        if(student.position.y<data.windowsHeight+10)
            student.position.y=student.position.y+10;
    }
}