import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Machine
{
    public long myMachineID;
    public String time;       //mandatory
    public String machineID;  //mandatory
    public int eventType;  //mandatory
    public String platformID;
    public float CPU;
    public float Memory;
    public float CPUleft;
    public float Memoryleft;

    public String server;


    public Machine(long mymachineID,String time, String machineID, int eventType, String platformID, float CPU, float Memory, String server)
    {
        this.myMachineID=mymachineID;
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
    public Machine(Machine m)
    {
        this.myMachineID=m.myMachineID;
        this.time=m.time;
        this.machineID=m.machineID;
        this.eventType=m.eventType;
        this.platformID=m.platformID;
        this.CPU=m.CPU;
        this.Memory=m.Memory;
        this.CPUleft=m.CPU;
        this.Memoryleft=m.Memory;

        this.server=m.server;
    }

    public float getCPULeft()
    {
        return CPUleft;
    }
    public float getMemoryleft()
    {
        return Memoryleft;
    }

    public static Machine UpdateMachine(Machine m, Task t)
    {
        Machine mUpdated=new Machine(m);


        mUpdated.CPUleft=m.CPUleft-t.CPUrequest;
        mUpdated.Memoryleft=m.Memoryleft-t.memoryRequest;

        return mUpdated;
    }


    public static ArrayList<Machine> MachineSorter(ArrayList<Machine> machines, String type, String typeOfOrder)
    {
        ArrayList<Machine> ret = new ArrayList<>();

        for(int i=0; i<machines.size(); i++)
        {
            Machine m=new Machine(machines.get(i));
            ret.add(m);
        }


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

    public static boolean FitsToMachine(Machine m, Task t)
    {
        if(m.CPUleft>t.CPUrequest && m.Memoryleft>t.memoryRequest)
            return true;
        return false;
    }

}

