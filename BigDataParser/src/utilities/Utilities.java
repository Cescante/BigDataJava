package utilities;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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
    
    public static void OutputError( Exception exp, String error )
    {
        String canonicalPath = null;
        
        try
        {
            canonicalPath = new File( "." ).getCanonicalPath();
        } catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        String filePath = String.format(
                "%s%s%s", canonicalPath, File.separator, "errors_" + Utilities.getDateTimeString() );

        try ( PrintWriter pw = new PrintWriter( new FileOutputStream( filePath, false ) ) )
        {
            exp.printStackTrace( pw );
            pw.println( error );
        }
        catch ( IOException e )
        {
            System.out.format( "Problem writing to path %s", filePath );
            e.printStackTrace();
        }
    }
}
