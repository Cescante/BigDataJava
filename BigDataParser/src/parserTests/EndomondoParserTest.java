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
    private String oneInstanceFilePath = "src/parserTests/Edmondo_1_instance.sql";
    private String oneInstanceLabel = "326940058";

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

        try ( BufferedReader br = new BufferedReader( new FileReader(
                absolutePath ) ) )
        {
            line = br.readLine();

        } catch ( IOException e )
        {
            TestCase.fail( e.getMessage() );
        }

        TestCase.assertNotNull( line );
        ArrayList<String> input = new ArrayList<String>( 1 );
        input.add( line );
        
        Collection<String> outLines = parser.ParseMany( input );
        TestCase.assertEquals( "There should be 8 entries in the workout data.", 8,
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
