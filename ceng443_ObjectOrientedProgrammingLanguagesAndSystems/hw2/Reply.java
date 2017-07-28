import java.util.List ;

public class Reply
{
  //=================================================================================================================================================

  private final String         description ;  // "Index", "Document" or "404"
  private final List< String > content     ;

  //=================================================================================================================================================

  public Reply ( String description , List< String > content )
  {
    this.description = description ;
    this.content     = content     ;
  }

  //=================================================================================================================================================

  @Override public String toString ()
  {
    // TO-DO

      String ret;

      String c="";

      if(content==null)
          ret="["+description+":NA]";
      else
      {
          for(int i=0; i<content.size(); i++)
          {
              c=c+content.get(i);
              if(!((i+1)==content.size()))
                  c=c+",";
          }
          ret="["+description+":"+c+"]";
      }

      return ret;
  }

  //=================================================================================================================================================
}


