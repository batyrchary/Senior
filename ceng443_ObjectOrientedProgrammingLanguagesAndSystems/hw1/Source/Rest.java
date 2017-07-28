import java.util.*;

public class Rest extends State {

    public Rest() {}

    @Override
    public void act(Student student, double deltaTime, Data data)
    {
        student.for_rest++;
        if(student.for_rest==student.for_rest_limit-20)
        {
            student.for_rest=0;
            student.state=student.previous_state;
        }
    }
}