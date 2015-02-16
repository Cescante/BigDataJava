package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import utilities.JSONUtilities;
import utilities.Utilities;

import com.sun.corba.se.impl.encoding.OSFCodeSetRegistry.Entry;
import com.sun.javafx.collections.MappingChange.Map;


/**
 * @author fqiao
 *          
 *          NOTE*: this DataParsers must be thread safe.
 * 
 */
public class EndomondoParsedDataParser extends FileDataParser<String>
{
    protected int batchSize = 2500;
    private ConcurrentHashMap<String, CopyOnWriteArraySet<Long>> userWorkoutMap;
    private ConcurrentHashMap<String, String> workoutIndexMap;

    /**
     * TODO: enable resume?
     * 
     * @param filePath
     */
    public EndomondoParsedDataParser( String filePath )
    {
        super( filePath );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<Long>>();
        this.workoutIndexMap = new ConcurrentHashMap<String, String>();
    }

    public EndomondoParsedDataParser( String filePath, int batchSize, int threadCount )
    {
        super( filePath, batchSize, threadCount );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<Long>>();
        this.workoutIndexMap = new ConcurrentHashMap<String, String>();
    }


    /**
     * Parse a file containing EndomondoData
     * 
     * @param postProcess
     */
    @Override
    public void ParseFile( PostProcessor postProcessor ) throws IOException
    {       
        try ( BufferedReader br = new BufferedReader( new FileReader( filePath ) ) )
        {
            ArrayList<String> rawLineBatch = new ArrayList<String>( batchSize );

            int lineNumber = 0;
            int batchIndex = 0;
                    
            // Read lines from file and send them to ParseMany in batches
            for ( String line; (line = br.readLine()) != null; )
            {
                rawLineBatch.add( line );

                // Parse a batch of lines and send them to the
                // postProcessor.
                if ( rawLineBatch.size() == batchSize )
                {
                    postProcessor.Process( this.ParseBatch( rawLineBatch, String.format( "%05d", batchIndex ) ), null );
                    rawLineBatch = new ArrayList<String>( batchSize );
                    batchIndex++;
                    
                    System.out.println( "Current line: " + lineNumber +", batch number: " + batchIndex );
                }
                
                lineNumber++;
            }

            // Finish off any danglers.
            if ( !rawLineBatch.isEmpty() )
            {
                postProcessor.Process( this.ParseBatch( rawLineBatch,  String.format( "%05d", batchIndex ) ), null );
                System.out.println( "Current line: " + lineNumber );
            }
        }
        
        List<String> userWorkouts = new ArrayList<String>( userWorkoutMap.size() );
        List<String> workoutIndices = new ArrayList<String>( workoutIndexMap.size() );
        
        for ( CopyOnWriteArraySet<Long> userWorkout : userWorkoutMap.values() )
        {
            userWorkouts.add( userWorkout.toString() );
        }
        
        for ( java.util.Map.Entry<String, String> workoutIndex : workoutIndexMap.entrySet() )
        {
            workoutIndices.add( String.format( "{\"%s\":\"%s\"", workoutIndex.getKey(), workoutIndex.getValue() ) );
        }
        
        ( (EndomondoParsedPostProcessor) postProcessor ).WriteDataToFile( userWorkoutMap.keySet(), userWorkouts, workoutIndices, null );
    }

    @Override
    public Collection<String> ParseMany( Collection<String> inputs, String batchIndex )
    {
        // DO NOT MAKE THIS GLOBAL. Not thread safe, must be local to ParseMany.
        JSONParser jparser = new JSONParser();
        
        for ( String workoutJSON : inputs )
        {
            workoutJSON = JSONUtilities.SanitizeJSON( workoutJSON );
            JSONObject workoutObject = null;
            
            try
            {
                workoutObject = (JSONObject) jparser.parse( workoutJSON );
            } catch ( ParseException e )
            {
                Utilities.OutputError( e, workoutJSON );
                
                System.exit( -1 );
            }
            
            // TODO: get user id and workout id, add them to maps
        }
        return null;
    }

    @Override
    protected String SanitizeLine( String line )
    {
        // TODO Auto-generated method stub
        return null;
    }
}
