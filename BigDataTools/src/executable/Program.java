package executable;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.*;

import parsers.*;
import parsers.PostProcessor.PostProcessType;

public class Program
{
    private static void ParseEndomondo( String inpath )
    {
        EndomondoDataParser dataParser = new EndomondoDataParser( inpath );
        File inFile = new File( dataParser.getFileName() );
        try
        {
            dataParser.ParseFile( new EndomondoPostProcessor( PostProcessType.WriteToFile, inFile.getParent() ) );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void ParseEndomondoParsed( String inpath )
    {
        EndomondoParsedDataParser dataParser = new EndomondoParsedDataParser( inpath );
        File inFile = new File( dataParser.getFileName() );
        try
        {
            dataParser.ParseFile( new EndomondoParsedPostProcessor( PostProcessType.WriteToFile, inFile.getParent() ) );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main( String[] args )
    {
        // TODO Auto-generated method stub
        Options options = new Options();
        options.addOption( "t", "datatype", true, "Type of data to parse:"
                + "\"EndoMondo\" - endomondo dataset." );
        options.addOption( "f", "inputfile", true, "The input file path" );
        
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "BigDataTools", options );
        
        CommandLineParser parser = new BasicParser();
        
        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse( options, args );
        }
        catch( ParseException exp )
        {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            System.exit( -1 );
        }
        
        String dataType = line.getOptionValue( "t" );
        
        if (dataType == null)
        {
            System.out.println( "No valid data type option, exiting." );
            System.exit(0);
        }
        
        dataType = dataType.toLowerCase();
        
        long startTime = System.nanoTime();
        
        switch( dataType )
        {
        case "endomondo":
            ParseEndomondo( line.getOptionValue( "f" ) );
            break;
        case "endomondoparsed":
            ParseEndomondoParsed( line.getOptionValue( "f" ) );
            break;
        default:
            System.out.println( "No valid data type option, exiting." );
        }
        
        long endTime = System.nanoTime();
        
        System.out.println( "Time elapsed: " + ( endTime - startTime ) );
    }
}
