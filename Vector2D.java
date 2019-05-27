public class Vector2D {

    public Vector2D(int xx, int yy) {
        x=xx;   y=yy;
    }

    public int x,y;

    public int getIntX() {
        return x;
    }

    public int getIntY() {

        return y;
    }

    public double distanceTo(Vector2D other) {
        return Math.sqrt((this.x-other.x)*(this.x-other.x) + (this.y-other.y)*(this.y-other.y));
    }

}