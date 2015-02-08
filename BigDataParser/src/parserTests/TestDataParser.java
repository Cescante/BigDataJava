package parserTests;

import java.util.*;

import parsers.DataParser;

/**
 * A simple test DataParser class.
 * 
 * @author fqiao
 *
 */
public class TestDataParser extends DataParser<String, String>
{
    public TestDataParser( int batchSize, int threadCount )
    {
        super( batchSize, threadCount );
    }

    // Generate random strings to parse.
    public static final String parsedFlag = "__Parsed__";
    
    @Override
    public Collection<String> ParseMany( Collection<String> inputs )
    {
        List<String> outputs = new ArrayList<String>( inputs.size() );
        
        // Just add the parsed Flag to the end of each string.
        for( String input : inputs )
        {
            outputs.add( input + parsedFlag );
        }
        
        return outputs;
    }
}
