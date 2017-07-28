import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;//import edip bilyan rahat zaten to-do di;


public class Server extends Thread
{
    private boolean isRunning;
    private final Map< String, List< String >> content;
    private final Queue< Request > requestQueue;

  //=================================================================================================================================================

    public int number_of_request_received=0;
    public int number_of_replies_sent=0;

    public String method="initial";
    public String uri="initial";
    public String parameter="initial";
    public Client client;

    // TO-DO: Optional extra fields

  //=================================================================================================================================================

    public Server (String name)
    {
        super(name);
        isRunning = true;
        content = new HashMap<>();
        generateContent();
        requestQueue = new LinkedList<>();

        Timer timer = new Timer();
        TimerTask timerTask = new TimerTask()
        {
            @Override public void run()
            {
                Runner.logf( "Timer    : Server shutting down...%n" ) ;

                isRunning = false ;

                // TO-DO: Optional extra statements here
                Request uyandirici=new Request(null,null,null,null);
                acceptRequest(uyandirici);

                timer.cancel() ;
            }
        };

        timer.schedule( timerTask , 10 * 1000 ) ;
        start() ;
    }

  //=================================================================================================================================================

  public boolean acceptRequest ( Request request )
  {
    // TO-DO
        modify_queue(false,request);
        return isRunning ;
  }

  //=================================================================================================================================================

  @Override public void run ()
  {
      // TO-DO

      while(isRunning)                                         //while running
      {
          Request request_handled = modify_queue(true, null);  //get request

          if(isRunning) {
              boolean isValid = isValid(request_handled);            //check if it is valid or not
              handle_request(request_handled, isValid);            //handle reqeust
          }
      }
      report();
  }

  //=================================================================================================================================================

  private void generateContent ()
  {
      String         key   ;
      List< String > value ;

      key = "A" ;  value = new ArrayList<>() ;  value.add( "A1" ) ;  value.add( "A2" ) ;  content.put( key , value ) ;
      key = "B" ;  value = new ArrayList<>() ;  value.add( "B1" ) ;  value.add( "B2" ) ;  content.put( key , value ) ;
      key = "C" ;  value = new ArrayList<>() ;  value.add( "C1" ) ;  value.add( "C2" ) ;  content.put( key , value ) ;
      key = "D" ;  value = new ArrayList<>() ;  value.add( "D1" ) ;  value.add( "D2" ) ;  content.put( key , value ) ;
  }

  //=================================================================================================================================================

  // TO-DO: Optional extra helper methods


    public synchronized Request modify_queue(boolean s_or_c, Request request) //servermiyim veya client
    {
        if(s_or_c) //server
        {
            while(requestQueue.isEmpty() && isRunning)
            {
                try{
                    Runner.logf("Server   : Request queue is empty, waiting...%n");
                    wait();
                    if(isRunning)
                        Runner.logf("Server   : Has just been notified, getting back to work...%n");
                }catch (InterruptedException e){}
            }
            return requestQueue.poll();
        }
        else    //client
        {
            if(isRunning)
            {

                requestQueue.add(request); // accept request takes request and puts it to Queue;
                number_of_request_received++;
            }
            notify();                       //serveri uyandir veya herangi birini uyandir
        }
        return null;
    }

    public void report()
    {
        synchronized (System.out)
        {
            Runner.logf("Server   : Reporting...%n");
            Runner.logf("           -Number of requests received = %d%n", number_of_request_received);
            Runner.logf("           -Number of replies sent      = %d%n", number_of_replies_sent);
        }
    }
    public boolean isValid(Request request)
    {
        Method m;
        boolean isValid=true;
        try {
            m=Request.class.getDeclaredMethod("isValid");
            m.setAccessible(true);
            isValid=(boolean) m.invoke(request);
        } catch (NoSuchMethodException e) {
            System.out.println("No such method");
        } catch (InvocationTargetException e) {
            System.out.println("Invocation target error line 144");
        } catch (IllegalAccessException e) {
            System.out.println("Illegal access to method");
        }
        return isValid;
    }

    public void reflect(Request request_handled)
    {
        Field[] fields=Request.class.getDeclaredFields();
        for(Field field : fields)
        {
            field.setAccessible(true);
            try {
                if (field.getName().equals("method"))
                    method = (String)field.get(request_handled);
                else if (field.getName().equals("uri"))
                    uri = (String)field.get(request_handled);
                else if (field.getName().equals("parameter"))
                    parameter = (String)field.get(request_handled);
                else if(field.getName().equals("client"))
                    client=(Client)field.get(request_handled);
            }catch (IllegalAccessException e){
                System.out.println("Could not make the field accessible ");
            }
        }
    }

    public void handle_request(Request request_handled, boolean isValid)
    {
        reflect(request_handled);

        if(isValid)
        {
            Runner.logf("Server   : Request "+request_handled.toString()+" is VALID handling...%n");
            if(method.equals("Get"))
            {
                handle_get();
            }
            else
            {
                handle_post();
            }
        }
        else
        {
            Runner.logf("Server   : Request "+request_handled.toString()+" is INVALID dropped...%n");
        }
    }

    public Reply generate_reply()
    {
        if(uri.equals("Index"))
        {
            List<String> rcontent=new ArrayList<>();

            for ( String key : content.keySet() ) {
                rcontent.add(key);
            }
            return (new Reply("Index",rcontent));
        }
        else if(content.get(uri)!=null)
        {
            return (new Reply("Document",content.get(uri)));
        }
        else
        {
            return (new Reply("404",null));
        }
    }

    public void handle_get()
    {
        Reply reply=generate_reply();
        try {
            client.acceptReply(reply);
            number_of_replies_sent++;
        } catch (Exception e) {
            System.out.println("Reply gonderemedi nedense");
        }
    }

    public void handle_post()
    {
        if(uri.equals("Z"))
        {
            Runner.logf("Server   : Post data "+parameter+" on "+uri+" has invalid target uri, ignored!%n");
        }
        else
        {
            Runner.logf("Server   : Post data "+parameter+" on "+uri+" handled successfully.%n");
        }
    }

  //=================================================================================================================================================
}

