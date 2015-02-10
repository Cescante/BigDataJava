package executable;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.*;

import parsers.*;
import parsers.PostProcessor.PostProcessType;

public class Program
{

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
        
        switch( dataType )
        {
        case "EndoMondo":
            EndomondoDataParser dataParser = new EndomondoDataParser( line.getOptionValue( "f" ) );
            String filePath = dataParser.getFileName();
            File inFile = new File( filePath );
            try
            {
                dataParser.ParseFile( new EndomondoPostProcessor( PostProcessType.WriteToFile, inFile.getParent() ) );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            break;
        default:
            System.out.println( "No valid data type option, exiting." );
        }
    }
}
