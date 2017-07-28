public class RegularFactory extends TokenFactory {

    public RegularFactory() {}

    @Override
    public Token createToken(int countt, int inx, int iny)
    {
       return  new Regular(countt, inx, iny);
    }
}