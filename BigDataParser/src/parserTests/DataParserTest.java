package parserTests;

import java.util.*;

// import junit.framework.*;
import junit.framework.TestCase;

// import parsers.*;

public class DataParserTest extends TestCase
{   
    public void testParseBatch()
    {
        int testSize = 200;
        
        List<String> testStrings = new ArrayList<String>( testSize );
        
        for( int i = 0; i < testSize; i++ )
        {
            testStrings.add( Integer.toString( i ) );
        }
        
        TestDataParser testParser = new TestDataParser( 200, 6 );
        
        Collection<String> outputs = testParser.ParseBatch( testStrings, null );
        
        TestCase.assertEquals( "All items should be parsed", testSize, outputs.size() );
        
        for( String parsedLine : outputs )
        {
            TestCase.assertTrue( String.format("Each parsed string should have the expected ending: %s", parsedLine ),
                    parsedLine.endsWith( TestDataParser.parsedFlag ) );
        }
    }
}
