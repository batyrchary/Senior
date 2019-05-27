import javax.swing.*;
import java.awt.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TokenFairy implements AnimationEntity {

    public TokenFairy(int h, int w,Data dataa) {
        data=dataa;
        int randomX = ThreadLocalRandom.current().nextInt(0, w);
        int randomY = ThreadLocalRandom.current().nextInt(100+1, h);
        position=new Vector2D(randomX, randomY);
        counter=0;  height=h;   width=w;
    }

    Data data;
    public int counter,height,width;
    public Vector2D position;

    public Token createToken()
    {
        int countt = ThreadLocalRandom.current().nextInt(5, 35);
        int hangisi = ThreadLocalRandom.current().nextInt(0, 3);
        return data.factory.get(hangisi).createToken(countt,position.getIntX(),position.getIntY());
    }

    public void draw(Graphics2D g2d) {
        ImageIcon icon = new ImageIcon("./fig/Tinkerbell-LookingLeft.png");
        icon.paintIcon(null, g2d, position.getIntX(), position.getIntY());
    }

    public void act(double deltaTime) {
        int randomX = ThreadLocalRandom.current().nextInt(50, width-50);
        int randomY = ThreadLocalRandom.current().nextInt(200+1, height-50);
        position=new Vector2D(randomX, randomY);

        if(data.tokens.size()<30)
            data.tokens.add(this.createToken());
    }
}