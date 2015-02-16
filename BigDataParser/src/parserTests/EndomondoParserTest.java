package parserTests;

import java.io.*;
import java.nio.file.NotDirectoryException;
import java.util.*;

import junit.framework.*;

import org.apache.commons.io.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import bigDataProperties.*;
import parsers.*;
import parsers.PostProcessor.PostProcessType;

public class EndomondoParserTest extends TestCase
{
    private String canonicalPath;
    private String threeInstanceFilePath = "src/parserTests/EndoMondo3Instances.sql";
    private String threeInstanceLabel = "327000000";

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
    
    public void testParseFile()
    {
        EndomondoDataParser parser = new EndomondoDataParser(
        		threeInstanceFilePath );
        
        EndomondoPostProcessor postProcessor = null;
        
		try
		{
			postProcessor = new EndomondoPostProcessor(
					PostProcessType.WriteToFile, canonicalPath );
		}
		catch ( NotDirectoryException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        TestCase.assertNotSame(
        		"Expect output directory to have been updated", canonicalPath, postProcessor.getOutFilePath() );
        
        try
        {
			parser.ParseFile( postProcessor );
		}
        catch (IOException e)
        {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        File testOutput = new File( postProcessor.getOutFilePath() );
        File[] listofFiles = testOutput.listFiles();
        
        TestCase.assertEquals("Expect there to be three files.", 3, listofFiles.length );
        
        try 
        {
			FileUtils.deleteDirectory( testOutput );
		}
        catch ( IOException e )
        {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void testParseMany()
    {
        String absolutePath = String.format( "%s/%s", canonicalPath,
        		threeInstanceFilePath );

        EndomondoDataParser parser = new EndomondoDataParser(
        		threeInstanceFilePath );

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
        
        Collection<String> outLines = parser.ParseMany( input, null );
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
        TestCase.assertEquals( "The sql number should equal.", threeInstanceLabel, jobj
                .get( EndomondoProperties.workoutID ).toString() );
    }
}
