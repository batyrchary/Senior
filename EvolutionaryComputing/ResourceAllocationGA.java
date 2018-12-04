


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class ResourceAllocationGA
{

    public static Random rand = new Random();
    public static ArrayList<Integer> probs=new ArrayList<>();
    public static ArrayList<String> servers=new ArrayList<>();
    //public static HashMap<Pair,Integer> latencies=new HashMap<>();
    public static HashMap<String,Integer> latencies=new HashMap<>();
    public static ArrayList<Pair> pairs=new ArrayList<>();
    public static HashMap<Integer,ArrayList<String> > serversSeenByProb=new HashMap<>();

    public static ArrayList<Machine> MachineEvents=new ArrayList<>();
    public static ArrayList<Task> TaskEvents=new ArrayList<>();

    //considered
    public static ArrayList<Integer> probsConsidered=new ArrayList<>();
    public static ArrayList<Task> TaskEventsConsidered=new ArrayList<>();

    public static ArrayList<Task> decreasingTasksSortedByCPU;
    public static ArrayList<Task> decreasingTasksSortedByMemory;
    public static ArrayList<Task> increasingTasksSortedByCPU;
    public static ArrayList<Task> increasingTasksSortedByMemory;

    public static ArrayList<Machine> MachinesSortedByMemoryLeft;
    public static  ArrayList<Machine> MachinesSortedByCPULeft;

    public static String XORtype;
    public static String mutationType;
    public static String selectionType;




    public static float crossoverProbability;
    public static float mutationProbability;
    public static int numberOfBitsInGene=4;
    public static int number_of_allocations=0;
    public static int numberOfHeur=4;


    public static int maximumNumberOfMachines;
    public static int maxLatencyInMap;

    public static class Pair
    {
        public int prob;
        public String server;

        public Pair(int p, String s)
        {
            this.prob=p;
            this.server=s;
        }
    }



    public static Population generateRandomPopulation(int numberOfIndividuals)
    {
        Population population=new Population();

        for (int p=0; p<numberOfIndividuals; p++)
        {
            Chromosome c=new Chromosome();

            // for (int i = 0; i < numberOfHeur; i++)
            for (int i = 0; i < number_of_allocations; i++)
            {
                Gene g=new Gene();
                for(int j=0; j<numberOfBitsInGene; j++)
                {
                    int random = (int)(Math.random() *10 + 1);
                    g.add(random%2);
                }
                c.add(g);
            }
          //  System.out.println("individual fitness="+p);
            c.fitness=individualFitness(c);
           // System.out.println("fitness="+c.fitness);
            population.add(c);
        }
        return population;
    }


    public static int ComputeScore(ArrayList<String> assignmentPairs, int openMachineSize)
    {
        int score=0;

        int maxLatency=maxLatencyInMap*assignmentPairs.size();

        int overallLatency=0;



        for(int i=0; i<assignmentPairs.size(); i++)
        {

           // System.out.println(assignmentPairs.get(i));

            if(latencies.containsKey(assignmentPairs.get(i)))
            {
                overallLatency=overallLatency+latencies.get(assignmentPairs.get(i));
            }
            else
            {
                overallLatency=overallLatency+500;
            }

        }

       // System.out.println("numberOfMachines="+openMachineSize+"\tassignmentPairs="+assignmentPairs.size()+"\tprobs.size="+probs.size()+"\toverallLatency="+overallLatency);

       // score=(maximumNumberOfMachines-openMachineSize);//2*((maxLatency-overallLatency)/10)+ (maximumNumberOfMachines-numberOfMachines);
        //score=maxLatency-overallLatency;
        score=(maximumNumberOfMachines-openMachineSize);
        return score;
    }



    public static int individualFitnessSecondApproach(Chromosome individual)
    {
        int score=0;
        int nextFitOffset=0;

        ArrayList<String> assignmentPairs=new ArrayList<>();
        ArrayList<Task> AllocatedTasks=new ArrayList<>();
        ArrayList<Machine> openMachines=new ArrayList<>();


        for(int i=0; i<number_of_allocations; i=i+numberOfBitsInGene)
        {
            Gene gene=individual.get(i);

            //00->0   01->1   10->10  11->11      000->0, 001->1, 010->10,    011->11, 100->100,
            int HeuristicCode=gene.get(0)*10+gene.get(1);
            int parameter=gene.get(2)*10+gene.get(3);

            int how_many_times_to_apply=(int)(gene.get(4)*(Math.pow(2,3) +gene.get(5)*Math.pow(2,2)+gene.get(6)*Math.pow(2,1)+gene.get(7)*Math.pow(2,0)));

            if(HeuristicCode==0 || HeuristicCode==1 || HeuristicCode==10 || HeuristicCode==11 ) //FirstFit, BestFit, NextFit,
            {
                Task taskToBeAssigned=null;
                if(parameter==0 ) //decreasingTasksSortedByCPU
                {
                    for(int t=0; t<decreasingTasksSortedByCPU.size(); t++)
                    {
                        if(!(AllocatedTasks.contains(decreasingTasksSortedByCPU.get(i))))
                        {
                            taskToBeAssigned=decreasingTasksSortedByCPU.get(i);
                            break;
                        }
                    }
                }
                else if(parameter==1)//decreasingTasksSortedByMemory
                {
                    for(int t=0; t<decreasingTasksSortedByMemory.size(); t++)
                    {
                        if(!(AllocatedTasks.contains(decreasingTasksSortedByMemory.get(i))))
                        {
                            taskToBeAssigned=decreasingTasksSortedByMemory.get(i);
                            break;
                        }
                    }
                }
                else if(parameter==10) //increasingTasksSortedByCPU
                {
                    for(int t=0; t<increasingTasksSortedByCPU.size(); t++)
                    {
                        if(!(AllocatedTasks.contains(increasingTasksSortedByCPU.get(i))))
                        {
                            taskToBeAssigned=increasingTasksSortedByCPU.get(i);
                            break;
                        }
                    }
                }
                else if(parameter==11) //increasingTasksSortedByMemory
                {
                    for(int t=0; t<increasingTasksSortedByMemory.size(); t++)
                    {
                        if(!(AllocatedTasks.contains(increasingTasksSortedByMemory.get(i))))
                        {
                            taskToBeAssigned=increasingTasksSortedByMemory.get(i);
                            break;
                        }
                    }
                }
                /////////////////////apply heuristics
                Machine machineToAllocate=null;


                if(HeuristicCode==0 ||HeuristicCode==11 )                //FirstFit
                {
                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachines.size(); m++)
                    {
                        if(Machine.FitsToMachine(openMachines.get(m),taskToBeAssigned))
                        {
                            machineToAllocate=openMachines.get(m);
                            NotFoundInOpen=false;
                            break;
                        }
                    }
                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachineEvents.size(); m++)
                        {
                            Machine machine = new Machine(MachineEvents.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                else if(HeuristicCode==1)           //BestFit
                {
                    ArrayList<Machine> openMachinesSortedByMemoryLeft=Machine.MachineSorter(openMachines,"MemoryLeft","NA");
                    // ArrayList<Machine> openMachinesSortedByCPULeft=MachineSorter(openMachines,"CPULeft","NA");

                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachinesSortedByMemoryLeft.size(); m++)
                    {
                        if(Machine.FitsToMachine(openMachinesSortedByMemoryLeft.get(m),taskToBeAssigned))
                        {
                            machineToAllocate=openMachinesSortedByMemoryLeft.get(m);
                            NotFoundInOpen=false;
                            break;
                        }
                    }
                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachinesSortedByMemoryLeft.size(); m++)
                        {
                            Machine machine = new Machine(MachinesSortedByMemoryLeft.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                else if(HeuristicCode==10)          //NextFit
                {
                    boolean NotFoundInOpen=true;
                    for(;nextFitOffset<openMachines.size();nextFitOffset++)
                    {
                        if(Machine.FitsToMachine(openMachines.get(nextFitOffset),taskToBeAssigned))
                        {
                            machineToAllocate=openMachines.get(nextFitOffset);
                            NotFoundInOpen=false;
                            break;
                        }
                    }   //dont have to increment nextFitOffset

                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachineEvents.size(); m++)
                        {
                            Machine machine = new Machine( MachineEvents.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                //   else if(HeuristicCode==11)
                // {

//                }

                AllocatedTasks.add(taskToBeAssigned);

                if(!openMachines.contains(machineToAllocate))
                {
                    openMachines.add(machineToAllocate);
                }

                int indexInOpen=openMachines.indexOf(machineToAllocate);



                if(machineToAllocate==null)
                    System.out.println("problem");
                openMachines.set(indexInOpen,Machine.UpdateMachine(machineToAllocate,taskToBeAssigned));


                int probID=taskToBeAssigned.probID;
                String serverId=machineToAllocate.server;

                String p="prb="+String.valueOf(probID)+",server="+serverId;
                assignmentPairs.add(p);
               // assignmentPairs.add(new Pair(probID,serverId));
            }
        }
        score=ComputeScore(assignmentPairs,openMachines.size());
        return score;
    }


    public static boolean ContainsTask(ArrayList<Task> tasks, Task task)
    {
        boolean contains=false;

        for(int i=0; i<tasks.size();i++)
        {
            if(tasks.get(i).taskID==task.taskID)
            {
                contains=true;
                break;
            }
        }

    return contains;
    }

    public static int individualFitness(Chromosome individual)
    {
        int score=0;
        int nextFitOffset=0;

    //    ArrayList<Machine> MachineEventsForFitness=new ArrayList<>();
    //    for(int i=0; i<MachineEvents.size();i++)
    //    {
     //       Machine m=MachineEvents.get(i);
     //       MachineEventsForFitness.add(new Machine(m));
     //   }

        ArrayList<String> assignmentPairs=new ArrayList<>();
        ArrayList<Task> AllocatedTasks=new ArrayList<>();
        ArrayList<Machine> openMachines=new ArrayList<>();

        for(int i=0; i<number_of_allocations; i=i+1)
        {
            Gene gene=individual.get(i);

            //00->0   01->1   10->10  11->11      000->0, 001->1, 010->10,    011->11, 100->100,
            int HeuristicCode=gene.get(0)*10+gene.get(1);
            int parameter=gene.get(2)*10+gene.get(3);

            if(HeuristicCode==0 || HeuristicCode==1 || HeuristicCode==10 || HeuristicCode==11 ) //FirstFit, BestFit, NextFit,
            {
                Task taskToBeAssigned=null;
                if(parameter==0 ) //decreasingTasksSortedByCPU
                {
                    for(int t=0; t<decreasingTasksSortedByCPU.size(); t++)
                    {
                        //if(!(AllocatedTasks.contains(decreasingTasksSortedByCPU.get(t))))
                        if(!ContainsTask(AllocatedTasks,decreasingTasksSortedByCPU.get(t)))
                        {
                            taskToBeAssigned=decreasingTasksSortedByCPU.get(t);
                            break;
                        }
                    }
                }
                else if(parameter==1)//decreasingTasksSortedByMemory
                {
                    for(int t=0; t<decreasingTasksSortedByMemory.size(); t++)
                    {
                        //if(!(AllocatedTasks.contains(decreasingTasksSortedByMemory.get(t))))
                        if(!ContainsTask(AllocatedTasks,decreasingTasksSortedByMemory.get(t)))
                        {
                            taskToBeAssigned=decreasingTasksSortedByMemory.get(t);
                            break;
                        }
                    }
                }
                else if(parameter==10) //increasingTasksSortedByCPU
                {
                    for(int t=0; t<increasingTasksSortedByCPU.size(); t++)
                    {
                        //if(!(AllocatedTasks.contains(increasingTasksSortedByCPU.get(t))))
                        if(!ContainsTask(AllocatedTasks,increasingTasksSortedByCPU.get(t)))
                        {
                            taskToBeAssigned=increasingTasksSortedByCPU.get(t);
                            break;
                        }
                    }
                }
                else if(parameter==11) //increasingTasksSortedByMemory
                {
                    for(int t=0; t<increasingTasksSortedByMemory.size(); t++)
                    {
                        //if(!(AllocatedTasks.contains(increasingTasksSortedByMemory.get(t))))
                        if(!ContainsTask(AllocatedTasks,increasingTasksSortedByMemory.get(t)))
                        {
                            taskToBeAssigned=increasingTasksSortedByMemory.get(t);
                            break;
                        }
                    }
                }

                /////////////////////apply heuristics
                Machine machineToAllocate=null;


                if(HeuristicCode==0 ||HeuristicCode==11 )// ||HeuristicCode==1 ||HeuristicCode==10 )                //FirstFit
                {
                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachines.size(); m++)
                    {
                        if(Machine.FitsToMachine(openMachines.get(m),taskToBeAssigned))
                        {
                            machineToAllocate=openMachines.get(m);
                            NotFoundInOpen=false;
                            break;
                        }
                    }
                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachineEvents.size(); m++)
                        {
                            Machine machine = new Machine(MachineEvents.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                else if(HeuristicCode==1)           //BestFit
                {
                    ArrayList<Machine> openMachinesSortedByMemoryLeft=Machine.MachineSorter(openMachines,"CPULeft","NA");
                   // ArrayList<Machine> openMachinesSortedByCPULeft=MachineSorter(openMachines,"CPULeft","NA");

                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachinesSortedByMemoryLeft.size(); m++)
                    {
                        if(Machine.FitsToMachine(openMachinesSortedByMemoryLeft.get(m),taskToBeAssigned))
                        {
                            machineToAllocate=openMachinesSortedByMemoryLeft.get(m);
                            NotFoundInOpen=false;
                            break;
                        }
                    }
                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachinesSortedByCPULeft.size(); m++)
                        {
                            Machine machine = new Machine(MachinesSortedByCPULeft.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                else if(HeuristicCode==10)          //NextFit
                {
                    boolean NotFoundInOpen=true;
                    for(;nextFitOffset<openMachines.size();nextFitOffset++)
                    {
                        if(Machine.FitsToMachine(openMachines.get(nextFitOffset),taskToBeAssigned))
                        {
                            machineToAllocate=openMachines.get(nextFitOffset);
                            NotFoundInOpen=false;
                            break;
                        }
                    }   //dont have to increment nextFitOffset

                    if(NotFoundInOpen)
                    {
                        for (int m = 0; m < MachineEvents.size(); m++)
                        {
                            Machine machine = new Machine( MachineEvents.get(m));
                            if (Machine.FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
             //   else if(HeuristicCode==11)
               // {

//                }


                AllocatedTasks.add(taskToBeAssigned);

                if(machineToAllocate==null) {
                    System.out.println("problem");

                }

                if(!openMachines.contains(machineToAllocate))
                {
                    openMachines.add(machineToAllocate);
                }

                int indexInOpen=openMachines.indexOf(machineToAllocate);

                openMachines.set(indexInOpen,Machine.UpdateMachine(machineToAllocate,taskToBeAssigned));

                int probID=taskToBeAssigned.probID;
                String serverId=machineToAllocate.server;

                String p="prb="+String.valueOf(probID)+",server="+serverId;
                assignmentPairs.add(p);
               // assignmentPairs.add(new Pair(probID,serverId));
            }
        }
        //System.out.println("Allocated="+AllocatedTasks.size());
       // System.out.println("OpenMach="+openMachines.size());
        score=ComputeScore(assignmentPairs, openMachines.size());
        return score;
    }

    public static float averageFitness(Population population)
    {
        int fitness=0;
        for(int i=0; i<population.size();i++)
        {
            fitness=fitness+population.get(i).fitness;
        }
        return fitness/population.size();
    }


    public static ArrayList<Chromosome> Crossover(Chromosome p1,Chromosome p2, String type)
    {
        ArrayList<Chromosome> ret=new ArrayList<>();
        Chromosome r1=new Chromosome(p1);
        Chromosome r2=new Chromosome(p2);

        int number_of_genes=p1.size();
        int number_of_bits_in_genes=p1.chromosome.get(0).size();
        int total_bits_in_chromosome=number_of_genes*number_of_bits_in_genes;

        int randomInt1=rand.nextInt(((total_bits_in_chromosome-1) - 0) + 1) + 0;
        int randomInt2=rand.nextInt(((total_bits_in_chromosome-1) - 0) + 1) + 0;

        if(randomInt1==randomInt2)
        {
            randomInt2=(randomInt1+10)%p1.size();
        }
        int max=randomInt1;
        int min=randomInt2;
        if(randomInt1<randomInt2)
        {
            max=randomInt2;
            min=randomInt1;
        }

        if(type.equals("SinglePoint"))
        {
            int index=0;
            for(int g=0;g<number_of_genes;g++)
            {
                for(int b=0;b<number_of_bits_in_genes;b++)
                {
                    if(index>=randomInt1)
                    {
                        int temp1=r1.chromosome.get(g).gene.get(b);
                        int temp2=r2.chromosome.get(g).gene.get(b);

                        r1.chromosome.get(g).gene.set(b,temp2);
                        r2.chromosome.get(g).gene.set(b,temp1);
                    }
                    index++;
                }
            }
        }
        else if(type.equals("TwoPoint"))
        {
            int index=0;
            for(int g=0;g<number_of_genes;g++)
            {
                for(int b=0;b<number_of_bits_in_genes;b++)
                {
                    if(index>=min && index<=max)
                    {
                        int temp1=r1.chromosome.get(g).gene.get(b);
                        int temp2=r2.chromosome.get(g).gene.get(b);

                        r1.chromosome.get(g).gene.set(b,temp2);
                        r2.chromosome.get(g).gene.set(b,temp1);
                    }
                    index++;
                }
            }
        }
        ret.add(r1);
        ret.add(r2);

        return ret;
    }

    public static Chromosome Mutation(Chromosome individual, String type)
    {

        Chromosome r1=new Chromosome(individual);

        int number_of_genes=r1.size();
        int number_of_bits_in_genes=r1.chromosome.get(0).size();
        int total_bits_in_chromosome=number_of_genes*number_of_bits_in_genes;

        int randomInt1=rand.nextInt(((total_bits_in_chromosome-1) - 0) + 1) + 0;

        if(type.equals("SingleBitFlip"))
        {
            int index=0;
            for(int g=0;g<number_of_genes;g++)
            {
                for (int b = 0; b < number_of_bits_in_genes; b++)
                {
                    if(randomInt1==index)
                    {
                        int temp1=r1.chromosome.get(g).gene.get(b);
                        r1.chromosome.get(g).gene.set(b,(temp1+1)%2); //flipping
                        return r1;
                    }
                    index++;
                }
            }
            System.out.println("problem in mutation1");
            return r1;
        }
        else {
            System.out.println("problem in mutation2");
            return individual;
        }
    }


    public static Population GenerateNewGeneration(Population generation)
    {
        Population newGeneration=new Population();

        for(int i=0; i<generation.size()/2; i++)
        {
            Chromosome parent1=IndividualSelection.RouletteWheel(generation);
            Chromosome parent2=IndividualSelection.RouletteWheel(generation);

            Chromosome kid1=new Chromosome(parent1);
            Chromosome kid2=new Chromosome(parent2);
            if(rand.nextFloat()<crossoverProbability)
            {
                ArrayList<Chromosome> kids = Crossover(parent1,parent2,XORtype);
                kid1 = kids.get(0);
                kid2 = kids.get(1);
            }
            if(rand.nextFloat()<mutationProbability)
            {
                kid1=Mutation(kid1,mutationType);
            }
            if(rand.nextFloat()<mutationProbability)
            {
                kid2=Mutation(kid2,mutationType);
            }

            kid1.fitness=individualFitness(kid1);
            kid2.fitness=individualFitness(kid2);

            newGeneration.add(kid1);
            newGeneration.add(kid2);
        }
        return newGeneration;
    }

    public static Population Selection(Population prevGeneration, Population newGeneration)
    {
        Population ret=prevGeneration.addTwoPopulation(prevGeneration,newGeneration);

        ret=ret.sortByFitness(ret);

        Population retf=new Population();

        for(int i=0; i<prevGeneration.size(); i++)
        {
            retf.add(ret.get(i));
        }
        return retf;
    }

    public static void GA(int population, int consecutiveGenerations, float minDifferenceInFitness)
    {
        float prevAvgFit=0;
        float newAvgFit=0;
        int consGeneration=0;


        Population initialPop=generateRandomPopulation(population);

        Population prevGeneration=initialPop;
        Population newGeneration=initialPop;


        prevAvgFit=averageFitness(prevGeneration);

        newAvgFit=averageFitness(newGeneration);

        while(consGeneration<consecutiveGenerations)
        {
            //System.out.println("Fitness="+prevAvgFit+"\tbestIndividual="+prevGeneration.get(0).fitness);
            System.out.println(prevAvgFit+","+prevGeneration.get(0).fitness);


            prevAvgFit=newAvgFit;

            newGeneration=GenerateNewGeneration(prevGeneration);

            newAvgFit=averageFitness(newGeneration);

            prevGeneration=Selection(prevGeneration,newGeneration);

            if(Math.abs(prevAvgFit-newAvgFit)<minDifferenceInFitness)
                consGeneration++;
            else
                consGeneration=0;
        }
    }


    public static void main(String[] args)
    {
        crossoverProbability=0.9f;
        mutationProbability=0.1f;

        //selectionType="FitnessProportionalSelection";// StochasticUniversalSampling
        mutationType="SingleBitFlip";
        XORtype="TwoPoint";  //TwoPoint,SinglePoint
        ////////////////////////////////////////

      

        String p1="/Users/batyrchary/Desktop/input/cloudU.csv";
        String p2="/Users/batyrchary/Desktop/input/m.csv";
        String p3="/Users/batyrchary/Desktop/input/tasks/";
        ReadData(p1,p2,p3);



        maximumNumberOfMachines=MachineEvents.size();
        System.out.println("maximumNumberOfMachines="+maximumNumberOfMachines);
        maxLatencyInMap=(Collections.max(latencies.values()));



        //number_of_allocations=probs.size();
        number_of_allocations=1000;
       // number_of_allocations=TaskEvents.size();

        System.out.println("maxlatency="+maxLatencyInMap*1000);


        numberOfBitsInGene=4;

        for(int i=0; i<number_of_allocations; i++)
        {
           // probsConsidered.add(probs.get(i));
            TaskEventsConsidered.add(TaskEvents.get(i));
        }

        decreasingTasksSortedByCPU=Task.TaskSorter(TaskEvents,"CPU","decreasing");
        decreasingTasksSortedByMemory=Task.TaskSorter(TaskEvents,"Memory","decreasing");
        increasingTasksSortedByCPU=Task.TaskSorter(TaskEvents,"CPU","increasing");
        increasingTasksSortedByMemory=Task.TaskSorter(TaskEvents,"Memory","increasing");


       // MachinesSortedByMemoryLeft=Machine.MachineSorter(MachineEvents,"MemoryLeft","NA");
        MachinesSortedByCPULeft=Machine.MachineSorter(MachineEvents,"CPULeft","NA");




        GA(50,10,0.5f); //population,consequtive generations fitness is not changing, difference in fitness
    }

    public static void ReadData(String ServerUserLatencyMapping, String machineEvents, String taskEvents)
    {
        ////
        try {
            File file = new File(ServerUserLatencyMapping);

            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                String splitted[] = line.replace(",,", ",").split(",");
                if (splitted[0].equals("prb"))
                    continue;

                int prb = Integer.parseInt(splitted[0]);
                ArrayList<String> serversForPrb=new ArrayList<>();
                for (int i = 1; i < splitted.length; i = i + 2) {
                    float latency = Float.parseFloat(splitted[i]);
                    String server = splitted[i + 1];

                    serversForPrb.add(server);
                    Pair pair = new Pair(prb, server);
                    pairs.add(pair);

                    int intlatency=Math.round(latency);

                    String p="prb="+String.valueOf(prb)+",server="+server;
                    latencies.put(p,intlatency);
                    //latencies.put(pair, intlatency);


                    if (!servers.contains(server))
                        servers.add(server);
                }

                serversSeenByProb.put(prb,serversForPrb);
                probs.add(prb);
            }
            br.close();
        }catch (Exception e){ System.out.println("exception in reading2"); }

        try
        {
            File file = new File(taskEvents);

            long taskID=0;
            for(File f: file.listFiles())
            {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line;
                int counter=0;
                while ((line = br.readLine()) != null)
                {
                    String splitted[]=line.split(",");

                    if(splitted.length>=11 && (!splitted[10].equals("")) && (!splitted[9].equals("")))
                    {

                        String time = splitted[0];                        //mandatory(int)
                        //    int missingInfo = Integer.parseInt(splitted[1]);
                        String jobID = splitted[2];                       //mandatory(int)
                        int taskIndex = Integer.parseInt(splitted[3]);    //mandatory
                        //    int machineID = Integer.parseInt(splitted[4]);
                        int eventType = Integer.parseInt(splitted[5]);    //mandatory
                        //    String user = splitted[6];
                        //    int schedulingClass = Integer.parseInt(splitted[7]);
                        int priority = Integer.parseInt(splitted[8]);     //mandatory
                        float CPUrequest = Float.parseFloat(splitted[9]);
                        float memoryRequest = Float.parseFloat(splitted[10]);

                      /*  if(CPUrequest*10<1 && memoryRequest*10<1) {
                            CPUrequest = CPUrequest * 10;
                            memoryRequest = memoryRequest * 10;
                        }*/
                        //    float diskSpaceRequest = Float.parseFloat(splitted[11]);
                        //    boolean differentMachineRestriction = Boolean.getBoolean(splitted[12]);

                        int probId=probs.get(counter%probs.size());
                        counter++;

                        TaskEvents.add(new Task(taskID,time,jobID,taskIndex,eventType,priority,CPUrequest,memoryRequest,probId));
                        taskID++;
                        //System.out.println(line);
                    }
                }
                br.close();
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("exception in reading taskevents");
        }

        try
        {
            File file = new File(machineEvents);

            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;


            int counter=0;
            long MyMachineId=0;
            while ((line = br.readLine()) != null) {

                String splitted []=line.split(",");

                String time;       //mandatory
                String machineID;  //mandatory
                int eventType;  //mandatory
                String platformID;
                float CPU;
                float Memory;

                if(splitted.length==6 && (!splitted[4].equals("")) && (!splitted[5].equals("")))
                {
                    time=splitted[0];
                    machineID=splitted[1];
                    eventType=Integer.parseInt(splitted[2]);
                    platformID=splitted[3];
                    CPU=Float.parseFloat(splitted[4]);
                    Memory=Float.parseFloat(splitted[5]);

                    String server=servers.get(counter%servers.size());
                    counter++;

                    MachineEvents.add(new Machine(MyMachineId,time,machineID,eventType,platformID,CPU,Memory,server));
                    MyMachineId++;
                }
            }
            br.close();
        }catch (Exception e){ System.out.println("exception in reading"); }
    }
}
