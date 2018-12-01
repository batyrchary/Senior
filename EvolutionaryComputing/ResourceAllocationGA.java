

import javax.crypto.Mac;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class ResourceAllocationGA {

    public static ArrayList<Integer> probs=new ArrayList<>();
    public static ArrayList<String> servers=new ArrayList<>();
    public static HashMap<Pair,Float> latencies=new HashMap<>();
    public static ArrayList<Pair> pairs=new ArrayList<>();
    public static HashMap<Integer,ArrayList<String> > serversSeenByProb=new HashMap<>();

    public static ArrayList<Machine> MachineEvents=new ArrayList<>();
    public static ArrayList<Task> TaskEvents=new ArrayList<>();

    //considered
    public static ArrayList<Integer> probsConsidered=new ArrayList<>();
    public static ArrayList<Task> TaskEventsConsidered=new ArrayList<>();


    public static int numberOfBitsInGene=4;
    public static int number_of_allocations=0;
    public static int numberOfHeur=4;

    public static class Task
    {
        String time;
        String jobID;
        int taskIndex;
        int eventType;
        int priority;
        float CPUrequest;
        float memoryRequest;

        public int probID;
        public Task(String time, String jobID, int taskIndex, int eventType, int priority, float CPUrequest, float memoryRequest, int probID)
        {
            this.time=time;
            this.jobID=jobID;
            this.taskIndex=taskIndex;
            this.eventType=eventType;
            this.priority=priority;
            this.CPUrequest=CPUrequest;
            this.memoryRequest=memoryRequest;

            this.probID=probID;
        }
        public  float getCPU()
        {
            return CPUrequest;
        }
        public  float getMemory()
        {
            return memoryRequest;
        }
    }

    public static class Machine
    {
        public String time;       //mandatory
        public String machineID;  //mandatory
        public int eventType;  //mandatory
        public String platformID;
        public float CPU;
        public float Memory;
        public float CPUleft;
        public float Memoryleft;

        public String server;


        public Machine(String time, String machineID, int eventType, String platformID, float CPU, float Memory, String server)
        {
            this.time=time;
            this.machineID=machineID;
            this.eventType=eventType;
            this.platformID=platformID;
            this.CPU=CPU;
            this.Memory=Memory;
            this.CPUleft=CPU;
            this.Memoryleft=Memory;

            this.server=server;
        }

        public float getCPULeft()
        {
            return CPUleft;
        }
        public float getMemoryleft()
        {
            return Memoryleft;
        }
    }

    public static class Pair
    {
        public int prob;
        public String server;
        // public float latency;
        public Pair(int p, String s)//, float l )
        {
            this.prob=p;
            this.server=s;
            //this.latency=l;
        }
    }


    public static class Gene
    {
        public ArrayList<Integer> gene=new ArrayList<>();

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


    public static class Chromosome
    {
        public ArrayList<Gene> chromosome=new ArrayList<>();

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
    }

    public static class Population
    {
        public ArrayList<Chromosome> chromosomes=new ArrayList<>();
        /* public Population(ArrayList<Chromosome> chromosomes)
         {
             this.chromosomes=chromosomes;
         }*/
        public void add(Chromosome c)
        {
            chromosomes.add(c);
        }

        public int size()
        {
            return this.chromosomes.size();
        }

        public Chromosome get(int i)
        {
            return chromosomes.get(i);
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
            population.add(c);
        }
        return population;
    }



    public static ArrayList<Machine> MachineSorter(ArrayList<Machine> machines, String type, String typeOfOrder)
    {
        ArrayList<Machine> ret = machines;

        if (type.equals("CPULeft")) {
            Collections.sort(ret, new Comparator<Machine>() {
                @Override
                public int compare(Machine p1, Machine p2) {
                    return (int) (p1.CPUleft - p2.CPUleft); // Ascending
                }
            });
            ret.sort(Comparator.comparingDouble(Machine::getCPULeft)); //smallest to largest
        }

        if (type.equals("MemoryLeft")) {
            Collections.sort(ret, new Comparator<Machine>() {
                @Override
                public int compare(Machine p1, Machine p2) {
                    return (int) (p1.Memoryleft - p2.Memoryleft); // Ascending
                }
            });
            ret.sort(Comparator.comparingDouble(Machine::getMemoryleft)); //smallest to largest
        }
        return ret;
    }


    public static ArrayList<Task> TaskSorter(ArrayList<Task> tasks, String type, String typeOfOrder)
    {
        ArrayList<Task> ret=tasks;

        if(type.equals("CPU"))
        {
            Collections.sort(ret, new Comparator<Task>() {
                @Override public int compare(Task p1, Task p2) {
                    return (int)(p1.CPUrequest - p2.CPUrequest); // Ascending
                }
            });
            ret.sort(Comparator.comparingDouble(Task::getCPU)); //smallest to largest
        }

        if(type.equals("Memory"))
        {
            Collections.sort(ret, new Comparator<Task>() {
                @Override public int compare(Task p1, Task p2) {
                    return (int)(p1.memoryRequest - p2.memoryRequest); // Ascending
                }
            });
            ret.sort(Comparator.comparingDouble(Task::getMemory)); //smallest to largest
        }

        if(typeOfOrder.equals("decreasing"))
        {
            Collections.reverse(ret);
        }
        return ret;
    }


    public static boolean FitsToMachine(Machine m, Task t)
    {
        if(m.CPUleft>t.CPUrequest && m.Memoryleft>t.memoryRequest)
            return true;
        return false;
    }
    public static Machine UpdateMachine(Machine m, Task t)
    {
        Machine mUpdated=m;

        mUpdated.CPUleft=m.CPUleft-t.CPUrequest;
        mUpdated.Memoryleft=m.Memoryleft-t.memoryRequest;

        return mUpdated;
    }

    public static float ComputeScore(ArrayList<Pair> assignmentPairs)
    {
        float score=0;
        int maximumNumberOfMachines=MachineEvents.size();
        float maxLatencyInMap=(Collections.max(latencies.values()));

        float maxLatency=maxLatencyInMap*assignmentPairs.size();

        int numberOfMachines=0;
        float overallLatency=0;

        HashSet noDupSet = new HashSet();


        for(int i=0; i<assignmentPairs.size(); i++)
        {
            noDupSet.add(assignmentPairs.get(i).server);
        }
        numberOfMachines=noDupSet.size();

        score=2*(maxLatency-overallLatency) + (maximumNumberOfMachines-numberOfMachines);

        return score;
    }


    public static float individualFitness(Chromosome individual)
    {
        float score=0;
        int nextFitOffset=0;

        ArrayList<Task> tasksTemp=TaskEventsConsidered;
        ArrayList<Pair> assignmentPairs=new ArrayList<>();


        ArrayList<Task> decreasingTasksSortedByCPU=TaskSorter(tasksTemp,"CPU","decreasing");
        ArrayList<Task> decreasingTasksSortedByMemory=TaskSorter(tasksTemp,"Memory","decreasing");
        ArrayList<Task> increasingTasksSortedByCPU=TaskSorter(tasksTemp,"CPU","increasing");
        ArrayList<Task> increasingTasksSortedByMemory=TaskSorter(tasksTemp,"Memory","increasing");

        ArrayList<Task> AllocatedTasks=new ArrayList<>();
        ArrayList<Machine> openMachines=new ArrayList<>();



        for(int i=0; i<number_of_allocations; i=i+numberOfBitsInGene)
        {
            Gene gene=individual.get(i);

            //00->0   01->1   10->10  11->11      000->0, 001->1, 010->10,    011->11, 100->100,
            int HeuristicCode=gene.get(0)*10+gene.get(1);
            int parameter=gene.get(2)*10+gene.get(3);

            if(HeuristicCode==0 || HeuristicCode==1 || HeuristicCode==10 || HeuristicCode==12 ) //FirstFit, BestFit, NextFit,
            {
                Task taskToBeAssigned=null;
                if(parameter==0) //decreasingTasksSortedByCPU
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

                if(HeuristicCode==0 || HeuristicCode==11)                //FirstFit
                {
                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachines.size(); m++)
                    {
                        if(FitsToMachine(openMachines.get(m),taskToBeAssigned))
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
                            Machine machine = MachineEvents.get(m);
                            if (!openMachines.contains(machine) && FitsToMachine(machine, taskToBeAssigned)) {
                                machineToAllocate = machine;
                                break;
                            }
                        }
                    }
                }
                else if(HeuristicCode==1)           //BestFit
                {
                    ArrayList<Machine> openMachinesSortedByMemoryLeft=MachineSorter(openMachines,"MemoryLeft","NA");
                   // ArrayList<Machine> openMachinesSortedByCPULeft=MachineSorter(openMachines,"CPULeft","NA");

                    boolean NotFoundInOpen=true;
                    for(int m=0; m<openMachinesSortedByMemoryLeft.size(); m++)
                    {
                        if(FitsToMachine(openMachinesSortedByMemoryLeft.get(m),taskToBeAssigned))
                        {
                            machineToAllocate=openMachinesSortedByMemoryLeft.get(m);
                            NotFoundInOpen=false;
                            break;
                        }
                    }
                    if(NotFoundInOpen)
                    {
                        ArrayList<Machine> MachinesSortedByMemoryLeft=MachineSorter(MachineEvents,"MemoryLeft","NA");
                       // ArrayList<Machine> MachinesSortedByCPULeft=MachineSorter(MachineEvents,"CPULeft","NA");

                        for (int m = 0; m < MachinesSortedByMemoryLeft.size(); m++)
                        {
                            Machine machine = MachinesSortedByMemoryLeft.get(m);
                            if (!openMachines.contains(machine) && FitsToMachine(machine, taskToBeAssigned)) {
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
                        if(FitsToMachine(openMachines.get(nextFitOffset),taskToBeAssigned))
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
                            Machine machine = MachineEvents.get(m);
                            if (!openMachines.contains(machine) && FitsToMachine(machine, taskToBeAssigned)) {
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
                int indexInMachineEvent=MachineEvents.indexOf(machineToAllocate);

                openMachines.set(indexInOpen,UpdateMachine(machineToAllocate,taskToBeAssigned));
                MachineEvents.set(indexInMachineEvent,UpdateMachine(machineToAllocate,taskToBeAssigned));


                int probID=taskToBeAssigned.probID;
                String serverId=machineToAllocate.server;
                assignmentPairs.add(new Pair(probID,serverId));
            }
        }
        score=ComputeScore(assignmentPairs);
        return score;
    }

    public static float averageFitness(Population population)
    {
        float fitness=0;
        for(int i=0; i<population.size();i++)
        {
            fitness=fitness+individualFitness(population.get(i));
        }
        return fitness/population.size();
    }



    public static void GA(int population, int consecutiveGenerations, float minDifferenceInFitness)
    {
        float prevAvgFit=0;
        float newAvgFit=0;
        int consGeneration=0;


        Population initialPop=generateRandomPopulation(population);
        System.out.println("generated random");

        System.out.println(averageFitness(initialPop));

  /*      while(consGeneration<consecutiveGenerations)
        {

            prevAvgFit=newAvgFit;
            newAvgFit=averageFitness(population);



            if(Math.abs(prevAvgFit-newAvgFit)<minDifferenceInFitness)
                consGeneration++;
            else
                consGeneration=0;
        }*/
    }


    public static void main(String[] args)
    {
        String p1="/Users/batyrchary/Desktop/projects/Sushil/cloudU.csv";
        String p2="/Users/batyrchary/Desktop/traceData/machineevents/part-00000-of-00001.csv";
        String p3="/Users/batyrchary/Desktop/traceData/taskeventsincomplete/";
        ReadData(p1,p2,p3);

        System.out.println("tasks="+TaskEvents.size());
        System.out.println("probs="+probs.size());
        System.out.println("tasks/probs="+TaskEvents.size()/probs.size());

        //number_of_allocations=probs.size();
        number_of_allocations=10;//100;

        for(int i=0; i<number_of_allocations; i++)
        {
            probsConsidered.add(probs.get(i));
            TaskEventsConsidered.add(TaskEvents.get(i));
        }

          GA(50,5,0.5f); //population,consequtive generations fitness is not changing, difference in fitness
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
                    latencies.put(pair, latency);
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
                        //    float diskSpaceRequest = Float.parseFloat(splitted[11]);
                        //    boolean differentMachineRestriction = Boolean.getBoolean(splitted[12]);

                        int probId=probs.get(counter%probs.size());
                        counter++;

                        TaskEvents.add(new Task(time,jobID,taskIndex,eventType,priority,CPUrequest,memoryRequest,probId));
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

                    MachineEvents.add(new Machine(time,machineID,eventType,platformID,CPU,Memory,server));
                }
            }
            br.close();
        }catch (Exception e){ System.out.println("exception in reading"); }




    }
}
