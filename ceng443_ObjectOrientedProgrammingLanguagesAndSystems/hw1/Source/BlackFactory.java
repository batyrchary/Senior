public class BlackFactory extends TokenFactory {

    public BlackFactory() {}

    @Override
    public Token createToken(int countt, int inx, int iny)
    {
        return  new Black(countt, inx, iny);
    }

}