import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public  class Population {
    public ArrayList<Chromosome> chromosomes = new ArrayList<>();

    public Population(ArrayList<Chromosome> chromosomes) {
        this.chromosomes = chromosomes;
    }
    public Population() { this.chromosomes=new ArrayList<>(); }




    public void add(Chromosome c) {
        chromosomes.add(c);
    }

    public int size() {
        return this.chromosomes.size();
    }

    public Chromosome get(int i) {
        return chromosomes.get(i);
    }


    public Population addTwoPopulation(Population p1, Population p2) {
        Population ret = new Population();

        for (int i = 0; i < p1.size(); i++) {
            ret.add(p1.get(i));
        }

        for (int i = 0; i < p2.size(); i++) {
            ret.add(p2.get(i));
        }
        return ret;
    }

    public Population sortByFitness(Population p1) {


        ArrayList<Chromosome> ret = new ArrayList<>();

        for(int i=0; i<p1.chromosomes.size(); i++)
        {
            ret.add(p1.get(i));
        }

        Collections.sort(ret, new Comparator<Chromosome>() {
            @Override
            public int compare(Chromosome p1, Chromosome p2) {
                return (int) (p1.fitness - p2.fitness); // Ascending
            }
        });
        ret.sort(Comparator.comparingDouble(Chromosome::getFitness)); //smallest to largest

        Collections.reverse(ret);
        return new Population(ret);
    }

}