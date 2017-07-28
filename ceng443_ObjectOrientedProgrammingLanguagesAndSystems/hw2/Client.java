import java.lang.reflect.Field;
import java.util.List;
import java.util.Random; //import edip bilyan rahat zaten to-do di



public class Client extends Thread
{
    private final Server   server;
    private final int      periodOfRequests;
    private final Random   random;

  //=================================================================================================================================================

  // TO-DO: Optional extra fields
    public String[] get_post={"Get","Post"};
    public List<String> index_array;
    public int number_of_replies_received=0;
    public int number_of_request_sent=0;
    public boolean isAwake=false;

    public String         description ;
    public List< String > content     ;
  //=================================================================================================================================================

    public Client ( String name , double frequencyOfRequests , Server server )
    {
        super( name ) ;

        this.server           = server;
        this.periodOfRequests = (int) ( 1000.0 / frequencyOfRequests );
        this.random           = new Random();

        start() ;
    }

  //=================================================================================================================================================

    public void acceptReply(Reply reply)throws Exception
    {
        // TO-DO

        number_of_replies_received++;
        reflect(reply);
        if(description.equals("Index"))
            index_array=content;

        Runner.logf("%s : Got reply %s%n",this.getName(),reply.toString());
    }

  //=================================================================================================================================================

    @Override public void run()
    {
        // TO-DO
        getindex();

        while (isAwake)
        {
            Request request=generateRequest();
            synchronized (System.err)
            {
                isAwake = server.acceptRequest(request);
                if (isAwake) {
                    Runner.logf("%s : Sent request %s%n", this.getName(), request.toString());
                    number_of_request_sent++;
                }
            }
            sleeper();
        }
        report();
    }

  //=================================================================================================================================================

    private Request generateRequest ()
    {
        // TO-DO
        Request generated;

        int rm=random.nextInt(2);   //POST mu GET mi
        int key=random.nextInt(index_array.size()); // int i = generator.nextInt(10) + 1; This generates integers in the range [1, 11)
        int value=100+random.nextInt(800); //post da yazcagimiz sey

        String method=get_post[rm];
        String uri=index_array.get(key);
        String parameter=null;
        if(rm==1)
            parameter="Data"+Integer.toString(value);

        if(1==random.nextInt(10))
            uri="Z";

        generated=new Request(this,method,uri,parameter);

        return generated;
    }

    //=================================================================================================================================================

    public void getindex()
    {

        Request request=new Request(this,"Get","Index",null);

        synchronized (System.err)
        {
            Runner.logf("%s : Sent request %s%n", this.getName(), request.toString());
            isAwake = server.acceptRequest(request);
        }
        number_of_request_sent++;


        sleeper();

        while(isAwake && index_array==null)
        {
            synchronized (System.err)
            {
                Runner.logf("%s : Sent request %s%n", this.getName(), request.toString());
                isAwake = server.acceptRequest(request);
            }

            number_of_request_sent++;
            sleeper();
        }
        if(!isAwake)
            report();
    }

    public void sleeper()
    {
        try {
            Thread.sleep(periodOfRequests);
        } catch (InterruptedException e) {
            System.out.println("cant sleep in get index");
        }
    }

    public void report()
    {
        synchronized (System.out)
        {
            Runner.logf("%s : Reporting...%n", this.getName());
            Runner.logf("           -Number of requests sent  = " + number_of_request_sent + "%n");
            Runner.logf("           -Number of replies received  = " + number_of_replies_received + "%n");
        }

    }

    public void reflect(Reply reply)
    {
        Field[] fields=Reply.class.getDeclaredFields();
        for(Field field : fields)
        {
            field.setAccessible(true);
            try {
                if (field.getName().equals("description"))
                    description = (String)field.get(reply);
                else if (field.getName().equals("content"))
                    content = (List<String>) field.get(reply);
            }catch (IllegalAccessException e){
                System.out.println("Client Could not make the field accessible ");
            }
        }
    }


    // TO-DO: Optional extra helper methods

    //=================================================================================================================================================

}

