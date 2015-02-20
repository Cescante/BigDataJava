package parsers;

import java.util.*;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.*;

import utilities.*;

/**
 * @author fqiao
 *
 *         Post processing logic for the parsers.
 *         
 *         Currently only contains logic for output to data structure or file.
 *
 */
public class PostProcessor
{   
    public enum PostProcessType
    {
        WriteToFile, StoreToCollection
    }
    
    protected Collection<String> outputCollection;
    
    public Collection<String> getOutputCollection() { return outputCollection; }
    
    protected PostProcessType thisType;
    
    public PostProcessType getPostProcessType() { return thisType; }
    
    protected String outFilePath;
    
    public String getOutFilePath() { return outFilePath; }
    
    protected Boolean batchedOutput;

    public Boolean getbatchedOutput() { return batchedOutput; }

    public PostProcessor( PostProcessType type, String outFilePath, Boolean batchedOutput ) throws NotDirectoryException
    {
        this.thisType = type;
        this.outFilePath = outFilePath;
        this.batchedOutput = batchedOutput;
        
        switch ( this.thisType )
        {
        case StoreToCollection:
            this.InitalizeStoreToCollection();
        case WriteToFile:
            this.InitializeWriteToFile();
        default:
            break;
        }
            
    }
    
    public PostProcessor( PostProcessType type ) throws NotDirectoryException
    {
        this( type, ".", true );
    }
    
    public void Process( Collection<String> outputs, String customFileIndex )
    {
        
        switch (this.thisType)
        {
        case StoreToCollection:
            this.StoreDataToCollection( outputs );
        case WriteToFile:
            this.WriteDataToFile( outputs, customFileIndex );
        default:
            break;
        }
    }
    
    public void OutputToFile( String outPath, Collection<String> outputs )
    {
        if ( outputs == null || outputs.isEmpty() )
        {
            return;
        }

        try ( PrintWriter pw = new PrintWriter( new FileOutputStream( outPath, !this.batchedOutput ) ) )
        {
            for( String jsonOut : outputs )
            {
                pw.println( jsonOut );
            }
        }
        catch ( IOException e )
        {
            System.out.format( "Problem writing to path %s", outPath );
            e.printStackTrace();
        }
    }
    
    protected void StoreDataToCollection( Collection<String> outputs )
    {
        outputCollection.addAll( outputs );
    }
    
    protected void WriteDataToFile( Collection<String> outputs, String customFileIndex )
    {
        return;
    }
    
    protected void InitalizeStoreToCollection()
    {
        this.outputCollection = new ArrayList<String>();
    }
    
    protected void InitializeWriteToFile() throws NotDirectoryException
    {
        if ( Files.isDirectory( FileSystems.getDefault().getPath( this.outFilePath ) ) )
        {
            if( this.outFilePath.charAt( this.outFilePath.length() - 1 ) != Constants.Slash )
            {
                this.outFilePath += Constants.Slash;
            }
            
            return;
        }
        else
        {
            throw new NotDirectoryException ( this.outFilePath );
        }
    }
}
