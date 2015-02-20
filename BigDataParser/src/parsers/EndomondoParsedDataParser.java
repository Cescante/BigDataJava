package parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import utilities.JSONUtilities;
import utilities.Utilities;
import bigDataProperties.*;

/**
 * @author fqiao
 *          
 *          NOTE*: this DataParsers must be thread safe.
 * 
 */
public class EndomondoParsedDataParser extends FileDataParser<String>
{
    protected int batchSize = 2500;
    private ConcurrentHashMap<String, CopyOnWriteArraySet<String>> userWorkoutMap;
    private ConcurrentHashMap<String, String> workoutIndexMap;
    private ConcurrentHashMap<String, AtomicInteger> dataKeysMap;

    /**
     * TODO: enable resume?
     * 
     * @param filePath
     */
    public EndomondoParsedDataParser( String filePath )
    {
        super( filePath );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<String>>();
        this.workoutIndexMap = new ConcurrentHashMap<String, String>();
        this.dataKeysMap = new ConcurrentHashMap<String, AtomicInteger>();
    }

    public EndomondoParsedDataParser( String filePath, int batchSize, int threadCount )
    {
        super( filePath, batchSize, threadCount );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<String>>();
        this.workoutIndexMap = new ConcurrentHashMap<String, String>();
        this.dataKeysMap = new ConcurrentHashMap<String, AtomicInteger>();
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
                String sanitizedLine = SanitizeLine( line );

                if ( sanitizedLine != null )
                {
                    rawLineBatch.add( line );
    
                    // Parse a batch of lines and send them to the
                    // postProcessor.
                    if ( rawLineBatch.size() == batchSize )
                    {
                        Collection<String> batchResult = this.ParseBatch( rawLineBatch,  String.format( "%05d", batchIndex ) );
                        if( postProcessor.batchedOutput )
                        {
                        	postProcessor.Process( batchResult, null );
                        }
                        rawLineBatch = new ArrayList<String>( batchSize );
                        batchIndex++;
                        
                        System.out.println( "Current line: " + lineNumber +", batch number: " + batchIndex );
                    }
                    
                    lineNumber++;
                }
            }

            // Finish off any danglers.
            if ( !rawLineBatch.isEmpty() )
            {
                Collection<String> batchResult = this.ParseBatch( rawLineBatch,  String.format( "%05d", batchIndex ) );
                if( postProcessor.batchedOutput )
                {
                	postProcessor.Process( batchResult, null );
                }
                System.out.println( "Current line: " + lineNumber );
            }
        }
        
        List<String> userWorkouts = new ArrayList<String>( userWorkoutMap.size() );
        List<String> workoutIndices = new ArrayList<String>( workoutIndexMap.size() );
        List<String> dataKeys = new ArrayList<String>( dataKeysMap.size() );
        
        for ( CopyOnWriteArraySet<String> userWorkout : userWorkoutMap.values() )
        {
            userWorkouts.add( userWorkout.toString() );
        }
        
        for ( java.util.Map.Entry<String, String> workoutIndex : workoutIndexMap.entrySet() )
        {
            workoutIndices.add( String.format( "%s:%s", workoutIndex.getKey(), workoutIndex.getValue() ) );
        }
        
        for ( java.util.Map.Entry<String, AtomicInteger> workoutKey : dataKeysMap.entrySet() )
        {
            dataKeys.add( String.format( "%s:%s", workoutKey.getKey(), workoutKey.getValue().intValue() ) );
        }
        
        ( (EndomondoParsedPostProcessor) postProcessor ).WriteDataToFile( userWorkoutMap.keySet(), userWorkouts, workoutIndices, dataKeys, null );
    }

    @Override
    public Collection<String> ParseMany( Collection<String> inputs, String batchIndex )
    {
        List<String> outputs = new ArrayList<String>( inputs.size() );
        
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

            String userId = (String)workoutObject.get( EndomondoProperties.userID );
            String workoutID = (String)workoutObject.get( EndomondoProperties.workoutID );
            
            for ( Object key : workoutObject.keySet() )
            {
                this.dataKeysMap.putIfAbsent( (String)key, new AtomicInteger( 0 ) );
                this.dataKeysMap.get( (String)key ).addAndGet( 1 );
            }

            // Don't do anything anymore if workoutID already processed.
            this.userWorkoutMap.putIfAbsent( userId, new CopyOnWriteArraySet<String>() );
            if( !this.userWorkoutMap.get( userId ).add( workoutID ) )
            {
                continue;
            }
            
            this.workoutIndexMap.putIfAbsent( workoutID, batchIndex );
            outputs.add( workoutJSON );
        }
        return outputs;
    }

    @Override
    protected String SanitizeLine( String line )
    {
        if ( line == null || line.length() == 0 )
        {
            return null;
        }
        return line;
    }
}
