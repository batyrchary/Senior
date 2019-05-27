public class FakeFactory extends TokenFactory {

    public FakeFactory() {}

    @Override
    public Token createToken(int countt, int inx, int iny)
    {
        return  new Fake(countt,  inx, iny);
    }

}