import java.util.ArrayList;
import java.util.Random;

public class IndividualSelection {



    public static ArrayList<Chromosome> StochasticUniversalSampling(Population generation)
    {
        int allfitness=0;
        for(int i=0; i<generation.size();i++)
        {
            allfitness=allfitness+generation.get(i).fitness;

        }

        ArrayList<Integer> randomInts=new ArrayList<>();
        while(randomInts.size()!=2*generation.size())
        {
            Random r = new Random();
            // System.out.println("allfitness="+allfitness);
            int randomNum=r.nextInt((allfitness - 0) + 1) + 0;

            if(!randomInts.contains(randomNum))
                randomInts.add(randomNum);
        }
        ArrayList<Chromosome> selected=new ArrayList<>();
        for(int r=0;r<randomInts.size();r++)
        {
            int wheel = 0;
            for (int i = 0; i < generation.size(); i++)
            {
                wheel = wheel + generation.get(i).fitness;

                if (randomInts.get(r) < wheel)
                {
                    selected.add(generation.get(i));
                    break;
                }
            }
        }
        return selected;
    }

    public static Chromosome RouletteWheel(Population generation)
    {

        int allfitness=0;
        for(int i=0; i<generation.size();i++)
        {
            allfitness=allfitness+generation.get(i).fitness;

        }

        Random r = new Random();
            // System.out.println("allfitness="+allfitness);
            int randomInt=r.nextInt((allfitness - 0) + 1) + 0;

            int wheel=0;
            for(int i=0; i<generation.size();i++)
            {
                wheel=wheel+generation.get(i).fitness;

                if(randomInt<wheel)
                    return generation.get(i);
            }
            System.out.println("Problem in individual selection1");
            return generation.get(generation.size()-1);
    }
}
