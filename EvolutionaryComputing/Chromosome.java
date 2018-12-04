import java.util.ArrayList;

public class Chromosome
{
    public ArrayList<Gene> chromosome=new ArrayList<>();
    public int fitness=0;



    public Chromosome(){}
    public Chromosome(Chromosome c)
    {
        for(int i=0; i<c.size();i++)
        {
            Gene g=new Gene(c.get(i));
            this.chromosome.add(g);
        }
    }

    public void add(Gene g)
    {
        chromosome.add(g);
    }
    public int size()
    {
        return chromosome.size();
    }
    public Gene get(int i)
    {
        return chromosome.get(i);
    }

    public int getFitness ()
    {
        return fitness;
    }
}