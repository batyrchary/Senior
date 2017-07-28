
import java.awt.*;
import java.util.*;

/**
 * 
 */
public interface AnimationEntity {

    /**
     * @param g2d
     */
    public void draw(Graphics2D g2d);

    /**
     * @param deltaTime
     */
    public void act(double deltaTime);

}