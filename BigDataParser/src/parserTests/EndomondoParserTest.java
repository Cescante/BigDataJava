package parserTests;

import java.io.*;
import java.util.*;

import junit.framework.*;

import org.json.simple.*;
import org.json.simple.parser.*;

import bigDataProperties.*;
import parsers.*;

public class EndomondoParserTest extends TestCase
{
    private String canonicalPath;
    private String oneInstanceFilePath = "src/parserTests/EndoMondo3Instances.sql";
    private String oneInstanceLabel = "327000000";

    private JSONParser jparser = new JSONParser();

    public EndomondoParserTest( String testName )
    {
        super( testName );

        try
        {
            canonicalPath = new File( "." ).getCanonicalPath();
        } catch ( IOException e1 )
        {
            e1.printStackTrace();
        }
    }

    protected void setUp() throws Exception
    {
        super.setUp();
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    public void testParseMany()
    {
        String absolutePath = String.format( "%s/%s", canonicalPath,
                oneInstanceFilePath );

        parsers.EndomondoDataParser parser = new EndomondoDataParser(
                oneInstanceFilePath );

        String line = null;

        ArrayList<String> input = new ArrayList<String>( 3 );
        
        try ( BufferedReader br = new BufferedReader( new FileReader(
                absolutePath ) ) )
        {
            while ( ( line = br.readLine() ) != null )
            {
                TestCase.assertNotNull( line );
                input.add( line );
            }

        } catch ( IOException e )
        {
            TestCase.fail( e.getMessage() );
        }
        
        Collection<String> outLines = parser.ParseMany( input );
        TestCase.assertEquals( "There should be 26 entries in the workout data.", 26,
                outLines.size() );

        Object testObject = null;

        try
        {
            testObject = jparser.parse( outLines.iterator().next() );
        } catch ( ParseException e )
        {
            TestCase.fail( e.getMessage() );
        }

        JSONObject jobj = (JSONObject) testObject;
        TestCase.assertEquals( "The sql number should equal.", oneInstanceLabel, jobj
                .get( EndomondoProperties.workoutID ).toString() );
        
//        try
//        {
//            PrintWriter testWriter = 
//                new PrintWriter("/Users/fqiao/Development/Thesis/testParseOuput.txt");
//            for( String outputJSON : outLines )
//            {
//                testWriter.println( outputJSON );
//            }
//            testWriter.close();
//        } catch ( FileNotFoundException e )
//        {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
}
