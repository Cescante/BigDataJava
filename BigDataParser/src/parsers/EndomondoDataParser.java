package parsers;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import org.jsoup.*;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

import utilities.*;
import bigDataProperties.*;

/**
 * @author fqiao
 *          
 *          NOTE*: this DataParsers must be thread safe.
 * 
 */
public class EndomondoDataParser extends FileDataParser<String>
{
    private ConcurrentHashMap<String, CopyOnWriteArraySet<Long>> userWorkoutMap;
    
    /**
     * Endomondo JSON constants
     */
    private static final String JSONlabelId = "id";
    private static final String JSONlabelStartVertexLat = "startVertexLat";
    private static final String JSONlabelEndVertexLat = "endVertexLat";
    private static final String JSONlabelStartVertexLng = "startVertexLng";
    private static final String JSONlabelEndVertexLng = "endVertexLng";
    private static final String JSONlabelRecords = "records";
    private static final String JSONlabelEncoded = "encoded";
    
    private static final String JSONlabelLaps = "laps";
    private static final String JSONlabelLapsAsc = "asc";
    private static final String JSONlabelLapsDur = "dur";
    private static final String JSONlabelLapsMaxPace = "maxPace";
    private static final String JSONlabelLapsDist = "dist";
    private static final String JSONlabelLapsAvgPace= "avgPace";
    private static final String JSONlabelLapsEndLat = "end_lat";
    private static final String JSONlabelLapsEndLng = "end_lng";
    private static final String JSONlabelLapsMaxAlt = "maxAlt";
    private static final String JSONlabelLapsMinAlt = "minAlt";
    private static final String JSONlabelLapsDesc = "desc";
    private static final String JSONlabelLapsPaths = "paths";
    
    private static final String JSONlabelData = "data";
    private static final String JSONlabelDataLng = "lng";
    private static final String JSONlabelDataLat = "lat";
    private static final String JSONlabelDataValues = "values";
    private static final String JSONlabelDataValuesDuration = "duration";
    private static final String JSONlabelDataValuesDistance = "distance";
    private static final String JSONlabelDataValuesPace = "pace";
    private static final String JSONlabelDataValuesAlt = "alt";
    

    /**
     * Pattern to determine whether the line is a valid SQL insert in to the
     * EndoMondoWorkouts table.
     */
    private static final String validLinePrefix = "INSERT INTO `EndoMondoWorkouts` VALUES (";
    
    protected int batchSize = 2500;

    /**
     * Endomondo HTML constants
     */
    private static final String classNameSportname = "sport-name";
    private static final String classNameDatetime = "date-time";
    private static final String classNameSummary = "summary";
    private static final String classNameDistance = "distance";
    private static final String classNameDuration = "duration";
    private static final String classNameAvgSpd = "avg-speed";
    private static final String classNameMaxSpd = "max-speed";
    private static final String classNameCalories = "calories";
    private static final String classNameAltitude = "altitude";
    private static final String classNameElvAsc = "elevation-asc";
    private static final String classNameElvDesc = "elevation-desc";
    private static final String classNamevalue = "value";

    /**
     * Pattern for matching one JSON workout instance
     */
    public static final String workoutMatchPattern = "(?s)(\\{\\n\\s+\"id\":\\s.+\\n\\})\\)\\;\\;\\}\\)\\;";

    /**
     * Pattern to Match one row in a SQL insert.
     */
    public static final String workoutSQLDelimitPattern = "\\(([0-9]+),'<[!]DOCTYPE\\shtml\\sPUBLIC";

    public static final String userIDPattern = "\\.\\./\\.\\./workouts/user/([0-9]+)";

    /**
     * The start of each HTML inserted offset by the ",'".
     * 
     * Also then end of final HTML inserted offset by the "')"
     */
    public static final int workoutRowHTMLTrimOffset = 2;

    public static final int workoutRowHTMLDelimitOffset = 3;

    /**
     * TODO: enable resume?
     * 
     * @param filePath
     */
    public EndomondoDataParser( String filePath )
    {
        super( filePath );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<Long>>();
    }

    public EndomondoDataParser( String filePath, int batchSize, int threadCount )
    {
        super( filePath, batchSize, threadCount );
        this.userWorkoutMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<Long>>();
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
                    
            // Read lines from file and send them to ParseMany in batches
            for ( String line; (line = br.readLine()) != null; )
            {
                String sanitizedLine = SanitizeLine( line );

                if ( sanitizedLine != null )
                {
                    rawLineBatch.add( sanitizedLine );

                    // Parse a batch of lines and send them to the
                    // postProcessor.
                    if ( rawLineBatch.size() == batchSize )
                    {
                        postProcessor.Process( this.ParseBatch( rawLineBatch, null ), null );
                        rawLineBatch = new ArrayList<String>( batchSize );
                        System.out.println( "Current line: " + lineNumber );
                    }
                }
                lineNumber++;
            }

            // Finish off any danglers.
            if ( !rawLineBatch.isEmpty() )
            {
                postProcessor.Process( this.ParseBatch( rawLineBatch, null ), null );
                System.out.println( "Current line: " + lineNumber );
            }
        }
        
        List<String> userWorkouts = new ArrayList<String>( userWorkoutMap.size() );
        
        for ( CopyOnWriteArraySet<Long> userWorkout : userWorkoutMap.values() )
        {
        	userWorkouts.add( userWorkout.toString() );
        }
        
        ( (EndomondoPostProcessor) postProcessor ).WriteDataToFile( userWorkoutMap.keySet(), userWorkouts, null );
    }

    /**
     * For each line of Endomondo data, match the JSON text with regex an parse to our
     * own JSON format.
     */
    @Override
    public Collection<String> ParseMany( Collection<String> inputs, String batchIndex )
    {
        List<String> outputs = new ArrayList<String>( inputs.size() );

        // DO NOT MAKE THIS GLOBAL. Not thread safe, must be local to ParseMany.
        JSONParser jparser = new JSONParser();

        // Process each input
        for ( String input : inputs )
        {

            String escaped = PatternUtils.unescape_perl_string( input );

            Matcher matcher = Pattern.compile( workoutSQLDelimitPattern )
                    .matcher( escaped );

            int previousRowHTMLIdx = 0;

            // process each item in input.
            while ( matcher.find() )
            {
                int rowIdx = matcher.start();
                String foundmatch = matcher.group();
                rowIdx += foundmatch.indexOf( ',' ) + workoutRowHTMLTrimOffset;

                if ( previousRowHTMLIdx > 0 )
                {
                    int previousRowEndIdx = matcher.start()
                            - workoutRowHTMLDelimitOffset;

                    String parsedJSONString = null;
                    
                    try
                    {
                        parsedJSONString = ParseOne( escaped.substring(
                                previousRowHTMLIdx, previousRowEndIdx ), jparser );
                    } catch ( ParseException e )
                    {
                        Utilities.OutputError( e, escaped.substring(
                                previousRowHTMLIdx, previousRowEndIdx ) );
                        
                        System.exit( -1 );
                    }

                    if( parsedJSONString != null )
                    {
                        outputs.add( parsedJSONString );
                    }
                }

                previousRowHTMLIdx = rowIdx;
            }

            // finish up final row
            String parsedJSONString = null;
            try
            {
                parsedJSONString = ParseOne(
                        escaped.substring(
                            previousRowHTMLIdx, 
                            escaped.length() - workoutRowHTMLTrimOffset ),
                        jparser );
            } catch ( ParseException e )
            {
                Utilities.OutputError( e, escaped.substring(
                        previousRowHTMLIdx,
                        escaped.length() - workoutRowHTMLTrimOffset ) );
                
                System.exit( -1 );
            }

            if( parsedJSONString != null )
            {
                outputs.add( parsedJSONString );
            }
        }

        return outputs;
    }

    @SuppressWarnings( "unchecked" )
    public String ParseOne( String htmlString, JSONParser jparser ) throws ParseException
    {
        JSONObject newObj = new JSONObject();

        // Extract JSON --------------------------------------------------
        JSONObject workoutObject = null;
        Matcher matcher = Pattern.compile( workoutMatchPattern ).matcher(
                htmlString );

        String workoutJSON = matcher.find() ? workoutJSON = matcher.group( 1 )
                : "";

        if( workoutJSON.equals( "" ) )
        {
            return null;
        }
        
        workoutJSON = JSONUtilities.SanitizeJSON( workoutJSON );
        
        workoutObject = (JSONObject) jparser.parse( workoutJSON );

        Long workoutID = (Long) workoutObject.get( JSONlabelId );
        Double workoutStartVertexLat = JSONUtilities.JSONSimpleGetWithDefault( workoutObject, JSONlabelStartVertexLat, 0.0 );
        Double workoutEndVertexLat = JSONUtilities.JSONSimpleGetWithDefault( workoutObject, JSONlabelEndVertexLat, 0.0 );
        Double workoutStartVertexLng = JSONUtilities.JSONSimpleGetWithDefault( workoutObject, JSONlabelStartVertexLng, 0.0 );
        Double workoutEndVertexLng = JSONUtilities.JSONSimpleGetWithDefault( workoutObject, JSONlabelEndVertexLng, 0.0 );
        JSONObject workoutRecords = (JSONObject) workoutObject.get( JSONlabelRecords );
        JSONArray workoutEncoded = (JSONArray) workoutObject.get( JSONlabelEncoded );
        
        JSONArray workoutData = (JSONArray) workoutObject.get( JSONlabelData );
        JSONArray workoutLaps = (JSONArray) workoutObject.get( JSONlabelLaps );
        
        JSONObject outputData = null;
        JSONObject outputLaps = null;
        
        // Build output object for workout Data.
        if ( workoutData != null )
        {
            outputData = new JSONObject();
            
            JSONArray outputDataLng = new JSONArray();
            JSONArray outputDataLat = new JSONArray();
            JSONArray outputDataDuration = new JSONArray();
            JSONArray outputDataDistance = new JSONArray();
            JSONArray outputDataPace = new JSONArray();
            JSONArray outputDataAlt = new JSONArray();
            
            for( Object workoutDatum : workoutData )
            {
                outputDataLng.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject) workoutDatum, JSONlabelDataLng, 0.0 ) );
                outputDataLat.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject) workoutDatum, JSONlabelDataLat, 0.0 ) );

                JSONObject inputDataValue = (JSONObject) ((JSONObject) workoutDatum).get( JSONlabelDataValues );
                Long outputDatumDuration = new Long( 0 );
                Double outputDatumDistance = 0.0;
                Double outputDatumPace = 0.0;
                Double outputDatumAlt = 0.0;
                
                if ( inputDataValue != null )
                {
                    outputDatumDuration = JSONUtilities.JSONSimpleGetWithDefault( inputDataValue, JSONlabelDataValuesDuration, (long)0 );
                    outputDatumDistance = JSONUtilities.JSONSimpleGetWithDefault( inputDataValue, JSONlabelDataValuesDistance, 0.0 );
                    outputDatumPace = JSONUtilities.JSONSimpleGetWithDefault( inputDataValue, JSONlabelDataValuesPace, 0.0 );
                    outputDatumAlt = JSONUtilities.JSONSimpleGetWithDefault( inputDataValue, JSONlabelDataValuesAlt, 0.0 );
                }
                outputDataDuration.add( outputDatumDuration );
                outputDataDistance.add( outputDatumDistance );
                outputDataPace.add( outputDatumPace );
                outputDataAlt.add( outputDatumAlt );
            }
            
            outputData.put( EndomondoProperties.dataPointsLng, outputDataLng );
            outputData.put( EndomondoProperties.dataPointsLat, outputDataLat );
            outputData.put( EndomondoProperties.dataPointsDuration, outputDataDuration );
            outputData.put( EndomondoProperties.dataPointsDistance, outputDataDistance );
            outputData.put( EndomondoProperties.dataPointsPace, outputDataPace );
            outputData.put( EndomondoProperties.dataPointsAlt, outputDataAlt );
        }
        
        // Build output object for laps
        if ( workoutLaps != null )
        {
            outputLaps = new JSONObject();
            
            JSONArray outputLapsAsc = new JSONArray();
            JSONArray outputLapsDur = new JSONArray();
            JSONArray outputLapsMaxPace = new JSONArray();
            JSONArray outputLapsDist = new JSONArray();
            JSONArray outputLapsAvgPace = new JSONArray();
            JSONArray outputLapsEndLat = new JSONArray();
            JSONArray outputLapsEndLng = new JSONArray();
            JSONArray outputLapsMaxAlt = new JSONArray();
            JSONArray outputLapsMinAlt = new JSONArray();
            JSONArray outputLapsDesc = new JSONArray();
            JSONArray outputLapsPaths = new JSONArray();
            
            for( Object workoutLap : workoutLaps )
            {
                outputLapsAsc.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsAsc, 0.0 ) );
                outputLapsDur.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsDur, (long)0 ) );
                outputLapsAsc.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsAsc, 0.0 ) );
                outputLapsMaxPace.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsMaxPace, 0.0 ) );
                outputLapsDist.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsDist, 0.0 ) );
                outputLapsAvgPace.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsAvgPace, 0.0 ) );
                outputLapsEndLat.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsEndLat, 0.0 ) );
                outputLapsEndLng.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsEndLng, 0.0 ) );
                outputLapsMaxAlt.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsMaxAlt, 0.0 ) );
                outputLapsMinAlt.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsMinAlt, 0.0 ) );
                outputLapsDesc.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsDesc, 0.0 ) );
                outputLapsPaths.add( JSONUtilities.JSONSimpleGetWithDefault( (JSONObject)workoutLap, JSONlabelLapsPaths, new JSONArray() ) );
            }
            
            outputLaps.put( EndomondoProperties.lapsAsc, outputLapsAsc );
            outputLaps.put( EndomondoProperties.lapsDur, outputLapsDur );
            outputLaps.put( EndomondoProperties.lapsMaxPace, outputLapsMaxPace );
            outputLaps.put( EndomondoProperties.lapsDist, outputLapsDist );
            outputLaps.put( EndomondoProperties.lapsAvgPace, outputLapsAvgPace );
            outputLaps.put( EndomondoProperties.lapsEndLat, outputLapsEndLat );
            outputLaps.put( EndomondoProperties.lapsEndLng, outputLapsEndLng );
            outputLaps.put( EndomondoProperties.lapsMaxAlt, outputLapsMaxAlt );
            outputLaps.put( EndomondoProperties.lapsMinAlt, outputLapsMinAlt );
            outputLaps.put( EndomondoProperties.lapsDesc, outputLapsDesc );
            outputLaps.put( EndomondoProperties.lapsPaths, outputLapsPaths );
        }

        // Extract MetaData from HTML------------------------------------------
        Document htmlDOM = Jsoup.parse( htmlString );
        matcher = Pattern.compile( userIDPattern ).matcher( htmlString );
        String userId = matcher.find() ? matcher.group( 1 ) : "";
        
        // Try inserting user and workout into the thread-safe map ------------
        // Parse only if the data is not a repeat.
        // TODO: No, parse if dates do not
        // repeat - solution: one more Map for timestamp verification.
        userWorkoutMap.putIfAbsent( userId, new CopyOnWriteArraySet<Long>() );
        if( !userWorkoutMap.get( userId ).add( workoutID ) )
        {
            return null;
        }
        
        // TODO: playlist information.
        Element sportNameDiv = htmlDOM.getElementsByClass( classNameSportname )
                .first();
        Element standardDetailsDiv = sportNameDiv.parent();
        Element summaryDiv = standardDetailsDiv.parent().parent();
        Element dateTimeDiv = standardDetailsDiv.getElementsByClass(
                classNameDatetime ).first();
        Element summaryUl = summaryDiv.getElementsByClass( classNameSummary )
                .first();
        Elements altitudeitems = summaryUl
                .getElementsByClass( classNameAltitude );


        String maxAltitude = null;
        String minAltitude = null;
        if ( altitudeitems.size() > 0 )
        {
            String altitude1 = altitudeitems.get( 0 )
                    .getElementsByClass( classNamevalue ).first().html();
            String altitude2 = altitudeitems.get( 1 )
                    .getElementsByClass( classNamevalue ).first().html();
    
            maxAltitude = Integer.parseInt( altitude1.split( "\\s" )[0] ) > Integer
                    .parseInt( altitude2.split( "\\s" )[0] ) ? altitude1
                    : altitude2;
            minAltitude = maxAltitude.equals( altitude1 ) ? altitude2
                    : altitude1;
        }

        String distance = GetHTMLSummaryItem( summaryUl, classNameDistance );
        String duration = GetHTMLSummaryItem( summaryUl, classNameDuration );
        String avgspd = GetHTMLSummaryItem( summaryUl, classNameAvgSpd );
        String maxspd = GetHTMLSummaryItem( summaryUl, classNameMaxSpd );
        String cal = GetHTMLSummaryItem( summaryUl, classNameCalories );
        String elvAsc = GetHTMLSummaryItem( summaryUl, classNameElvAsc );
        String elvDesc = GetHTMLSummaryItem( summaryUl, classNameElvDesc );

        // Construct new object ------------------------------------------
        newObj.put( EndomondoProperties.userID, userId );
        newObj.put( EndomondoProperties.workoutID, workoutID );
        newObj.put( EndomondoProperties.workoutSportName, sportNameDiv.html() );
        newObj.put( EndomondoProperties.workoutDate, dateTimeDiv.html() );
        newObj.put( EndomondoProperties.workoutDistance, distance );
        newObj.put( EndomondoProperties.workoutDuration, duration );
        newObj.put( EndomondoProperties.workoutAverageSpeed, avgspd );
        newObj.put( EndomondoProperties.workoutMaxSpeed, maxspd );
        newObj.put( EndomondoProperties.workoutCalories, cal );
        newObj.put( EndomondoProperties.workoutMaxAltitude, maxAltitude );
        newObj.put( EndomondoProperties.workoutMinAltitude, minAltitude );
        newObj.put( EndomondoProperties.workoutTotalAscent, elvAsc );
        newObj.put( EndomondoProperties.workoutTotalDescent, elvDesc );

        newObj.put( EndomondoProperties.dataStartVertexLat, workoutStartVertexLat );
        newObj.put( EndomondoProperties.dataEndVertexLat, workoutEndVertexLat );
        newObj.put( EndomondoProperties.dataStartVertexLng, workoutStartVertexLng );
        newObj.put( EndomondoProperties.dataEndVertexLng, workoutEndVertexLng );
        newObj.put( EndomondoProperties.dataRecords, workoutRecords );
        newObj.put( EndomondoProperties.dataEncoded, workoutEncoded );
        newObj.put( EndomondoProperties.dataPoints, outputData );
        newObj.put( EndomondoProperties.laps, outputLaps );

        return newObj.toJSONString();
    }

    /**
     * Sanitize a line of the input file for parsing.
     * 
     * @param line
     *            the line to sanitize
     * @return A sanitized string ready for parsing, if line is a proper line of
     *         data. null Otherwise.
     * 
     */
    @Override
    protected String SanitizeLine( String line )
    {
        if ( line.startsWith( validLinePrefix ) )
        {
            return line;
        }
        return null;
    }

    private String GetHTMLSummaryItem( Element container, String className )
    {
        try
        {
            return container.getElementsByClass( className ).first()
                    .getElementsByClass( classNamevalue ).first().html();
        } catch ( NullPointerException exp )
        {
            return "";
        }
    }
}
