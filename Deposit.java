public class Deposit extends State {

    public Deposit() {}

    @Override
    public void act(Student student, double deltaTime, Data data)
    {
        student.for_rest++;
        if(student.for_rest==student.for_rest_limit)
        {
            student.for_rest=0;     student.previous_state=student.state;       student.state=new Rest();
        }
        else {
            int w = data.windowsWidth / 8;

            int desx = student.hangi_derse_dogru * w - w / 2;

            if(desx<(w/2))
                desx=w/2;

            int desy = 50;
            int curx = student.position.x;
            int cury = student.position.y;

            ///////unit
            double den = Math.sqrt((desx - curx) * (desx - curx) + (desy - cury) * (desy - cury));
            double x = (desx - curx) / den;
            double y = (desy - cury) / den;
            ///////

            curx = (int) (curx + x * student.speedd);
            cury = (int) (cury + y * student.speedd);

            student.position.x = curx;
            student.position.y = cury;

            if (Math.abs(desy - cury) < 10 && Math.abs(desx - curx) < 10) {
                int mtoken = data.courses.get(student.hangi_derse_dogru - 1).minTokens;
                int eskiden = student.hangi_derse_kac_basti[student.hangi_derse_dogru - 1];
                int toplam = eskiden + student.tokens_currently_have;

                student.tokens_currently_have = 0;
                student.hangi_derse_kac_basti[student.hangi_derse_dogru - 1] = toplam;

                if (mtoken <= toplam) {
                    data.courses.get(student.hangi_derse_dogru - 1).currently_enrolled_number++;
                    student.number_of_courses_taken = student.number_of_courses_taken + 1;
                    if (student.number_of_courses_taken == 2) {
                        student.state = new Leave();
                        student.id = "leave";
                    } else {
                        student.ilkders=student.hangi_derse_dogru;
                        student.state = new Seek();
                        student.id = "seek";
                    }

                } else {
                    student.state = new Seek();
                    student.id = "seek";
                }
            }
        }
    }
}