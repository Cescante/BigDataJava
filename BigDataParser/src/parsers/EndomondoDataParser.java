package parsers;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.*;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
// import org.jsoup.*;
import org.jsoup.nodes.*;
// import org.jsoup.parser.*;
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
    
    private static final String JSONlabelid = "id";

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

    public EndomondoDataParser( String filePath )
    {
        super( filePath );
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
                        this.ParseMany( rawLineBatch );
                        postProcessor.Process( rawLineBatch );
                        rawLineBatch = new ArrayList<String>( batchSize );
                    }
                }
            }

            // Finish off any danglers.
            if ( !rawLineBatch.isEmpty() )
            {
                this.ParseMany( rawLineBatch );
                postProcessor.Process( rawLineBatch );
            }
        }
        
        List<String> userWorkouts = new ArrayList<String>( userWorkoutMap.size() );
        
        for ( CopyOnWriteArraySet<Long> userWorkout : userWorkoutMap.values() )
        {
        	userWorkouts.add( StringUtils.join( ", ", userWorkout ) );
        }
        
        ( (EndomondoPostProcessor) postProcessor ).WriteDataToFile( userWorkoutMap.keySet(), userWorkouts, null );
    }

    @Override
    public Collection<String> ParseMany( Collection<String> inputs )
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

                    String parsedJSONString = ParseOne( escaped.substring(
                            previousRowHTMLIdx, previousRowEndIdx ), jparser );

                    if( parsedJSONString != null )
                    {
                        outputs.add( parsedJSONString );
                    }
                }

                previousRowHTMLIdx = rowIdx;
            }

            // finish up final row
            String parsedJSONString = ParseOne(
                    escaped.substring(
                        previousRowHTMLIdx, 
                        escaped.length() - workoutRowHTMLTrimOffset ),
                    jparser );

            if( parsedJSONString != null )
            {
                outputs.add( parsedJSONString );
            }
        }

        return outputs;
    }

    @SuppressWarnings( "unchecked" )
    public String ParseOne( String htmlString, JSONParser jparser )
    {
        JSONObject newObj = new JSONObject();

        // Extract JSON --------------------------------------------------
        JSONObject workoutObject = null;
        Matcher matcher = Pattern.compile( workoutMatchPattern ).matcher(
                htmlString );

        String workoutJSON = matcher.find() ? workoutJSON = matcher.group( 1 )
                : "";

        try
        {
            workoutObject = (JSONObject) jparser.parse( workoutJSON );
        } catch ( ParseException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Long workoutID = (Long) workoutObject.get( JSONlabelid );

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

        String altitude1 = altitudeitems.get( 0 )
                .getElementsByClass( classNamevalue ).first().html();
        String altitude2 = altitudeitems.get( 1 )
                .getElementsByClass( classNamevalue ).first().html();

        String maxAltitude = Integer.parseInt( altitude1.split( "\\s" )[0] ) > Integer
                .parseInt( altitude2.split( "\\s" )[0] ) ? altitude1
                : altitude2;
        String minAltitude = maxAltitude.equals( altitude1 ) ? altitude2
                : altitude1;

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
        newObj.put( EndomondoProperties.workoutData, workoutObject );

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
    public String SanitizeLine( String line )
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
