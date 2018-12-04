import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Task
{
    long taskID;
    String time;
    String jobID;
    int taskIndex;
    int eventType;
    int priority;
    float CPUrequest;
    float memoryRequest;

    public int probID;
    public Task(long taskID,String time, String jobID, int taskIndex, int eventType, int priority, float CPUrequest, float memoryRequest, int probID)
    {
        this.taskID=taskID;
        this.time=time;
        this.jobID=jobID;
        this.taskIndex=taskIndex;
        this.eventType=eventType;
        this.priority=priority;
        this.CPUrequest=CPUrequest;
        this.memoryRequest=memoryRequest;

        this.probID=probID;
    }

    public Task(Task t)
    {
        this.taskID=t.taskID;
        this.time=t.time;
        this.jobID=t.jobID;
        this.taskIndex=t.taskIndex;
        this.eventType=t.eventType;
        this.priority=t.priority;
        this.CPUrequest=t.CPUrequest;
        this.memoryRequest=t.memoryRequest;

        this.probID=t.probID;
    }


    public  float getCPU()
    {
        return CPUrequest;
    }
    public  float getMemory()
    {
        return memoryRequest;
    }


    public static ArrayList<Task> TaskSorter(ArrayList<Task> tasks, String type, String typeOfOrder)
    {
        ArrayList<Task> ret=new ArrayList<>();

        for(int i=0; i<tasks.size(); i++)
        {
            Task t=new Task(tasks.get(i));
            ret.add(t);
        }

        if(type.equals("CPU"))
        {
            Collections.sort(ret, new Comparator<Task>() {
                @Override public int compare(Task p1, Task p2) {
                  //  System.out.println(p1.CPUrequest+"\t"+p2.CPUrequest);
                    double roundOff1=p1.CPUrequest/10.0;
                    double roundOff2=p2.CPUrequest/10.0;
                   // double roundOff1 = Math.round(p1.CPUrequest * 100.0) / 100.0;
                   // double roundOff2 = Math.round(p2.CPUrequest * 100.0) / 100.0;
                    return (int)(roundOff1 - roundOff2); // Ascending
                   // return (int)(p1.CPUrequest - p2.CPUrequest); // Ascending
                }
            });
            ret.sort(Comparator.comparingDouble(Task::getCPU)); //smallest to largest
        }

        if(type.equals("Memory"))
        {
            Collections.sort(ret, new Comparator<Task>() {
                @Override public int compare(Task p1, Task p2) {

                    double roundOff1=p1.memoryRequest/10.0;
                    double roundOff2=p2.memoryRequest/10.0;
                   // double roundOff1 = Math.round(p1.memoryRequest * 100.0) / 100.0;
                   // double roundOff2 = Math.round(p2.memoryRequest * 100.0) / 100.0;
                    return (int)(roundOff1 - roundOff2); // Ascending
                    //return (int)(p1.memoryRequest - p2.memoryRequest); // Ascending
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
}