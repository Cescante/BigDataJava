package utilities;

import java.text.SimpleDateFormat;
import java.util.*;

public class Utilities
{
    public static String getDateTimeString()
    {
        Date date = new Date() ;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd.HH_mm_ss_SSS") ;
        return dateFormat.format( date );
    }
    
    public static <T> List<T> CollectionToList( Collection<T> collection )
    {
        List<T> list;
        if (collection instanceof List)
        {
            list = (List<T>) collection;
        }
        else
        {
            list = new ArrayList<T>(collection);
        }
        
        return list;
    }
    
    public static void JoinAll( Thread[] threads )
    {
        for( Thread thread : threads )
        {
            try
            {
                thread.join();
            } catch ( InterruptedException e )
            {
                System.out.println( "A thread got interrupted." );
                e.printStackTrace();
            }
        }
    }
}
