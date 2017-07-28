import java.util.concurrent.ThreadLocalRandom;

public class Seek extends State {

    public Seek() {}

    @Override
    public void act(Student student, double deltaTime, Data data)
    {
        student.for_rest++;
        if(student.for_rest==student.for_rest_limit)
        {
            student.for_rest=0;         student.previous_state=student.state;       student.state=new Rest();
        }
        else {
            double min = Double.MAX_VALUE;      int index = 0;      boolean aldi = false;

            for (int j = 0; j < data.tokens.size(); j++) {
                double ret = Math.abs(student.position.distanceTo(data.tokens.get(j).position));
                if (ret < min) {
                    min = ret;          index = j;
                }
                if (ret <= 7) {
                    aldi = true;        break;
                }
            }
            if (aldi) {
                student.tokens_currently_have = data.tokens.get(index).getCount();
                student.hangi_derse_dogru = ThreadLocalRandom.current().nextInt(1, 9);

                if(student.ilkders==student.hangi_derse_dogru)
                    student.hangi_derse_dogru=(student.hangi_derse_dogru+1)%9;

                data.tokens.remove(index);      student.state = new Deposit();      student.id = "deposit";

            } else {
                Vector2D cur = student.position;        Vector2D des = data.tokens.get(index).position;

                double x = (des.getIntX() - cur.getIntX()) / cur.distanceTo(des);
                double y = (des.getIntY() - cur.getIntY()) / cur.distanceTo(des);

                cur.x = (int) (cur.x + x * student.speedd);
                cur.y = (int) (cur.y + y * student.speedd);
                student.position.x = cur.x;     student.position.y = cur.y;

            }
        }
    }
}