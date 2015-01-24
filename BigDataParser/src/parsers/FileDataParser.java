package parsers;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 
 * @author fqiao
 *
 *         Parses all data in a File
 *
 * @param <E>
 *            The output type of the parser
 */
public abstract class FileDataParser<E> extends DataParser<String, E>
{
    protected String filePath;
    protected int batchSize = 1000;
    protected int threadCount = 10;

    public String getFileName()
    {
        return filePath;
    }

    public FileDataParser( String filePath )
    {
        this( filePath, 0, 0 );
    }

    /**
     * Initializes an instance of the FileDataParser.
     * 
     * @param filePath
     * @param batchSize
     * @param threadCount
     */
    public FileDataParser( String filePath, int batchSize, int threadCount )
    {
        try
        {
            String canonicalPath = new File( "." ).getCanonicalPath();
            filePath = String.format( "%s%s%s", canonicalPath, File.separator,  filePath );
        }
        catch ( IOException e1 )
        {
            e1.printStackTrace();
        }
        
        this.filePath = filePath;
        
        if( batchSize > 0 )
        {
            this.batchSize = batchSize;
        }
        
        if( threadCount > 0 )
        {
            this.threadCount = threadCount;
        }
    }

    public abstract void ParseFile( PostProcessor postProcessor )
            throws IOException;

    public abstract Collection<E> ParseMany( Collection<String> inputs );

    public abstract String SanitizeLine( String line );
}
