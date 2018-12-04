import java.util.ArrayList;

public class Gene
{
    public ArrayList<Integer> gene=new ArrayList<>();


    public Gene(){}
    public Gene(Gene g)
    {
        for(int i=0; i<g.size();i++)
            this.gene.add(g.get(i));
    }
    public void add(int bit)
    {
        gene.add(bit);
    }
    public int size()
    {
        return gene.size();
    }
    public int get(int i)
    {
        return gene.get(i);
    }
}
