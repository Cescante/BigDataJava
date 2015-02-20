package parsers;

import java.io.*;
import java.nio.file.NotDirectoryException;
import java.util.Collection;

import utilities.*;

public class EndomondoPostProcessor extends PostProcessor
{
    protected static String outFileExtension = ".json";
    protected static String usersFileName = "users";
    protected static String userWorkoutsFileName = "user.workouts";
    protected static String workoutsFileName = "workouts";
    protected static String directoryName = "EndomondoData.";
    
    public EndomondoPostProcessor( PostProcessType type, String outFilePath, Boolean batchedOutput ) 
            throws NotDirectoryException
    {
        super( type, outFilePath, batchedOutput );
    }
    
    /**
     * Override of the PostProcessor WriteDataToFile, outputs a batch
     * of parsed workouts to file.
     * 
     * @param outputs
     */
    @Override
    public void WriteDataToFile( Collection<String> outputs, String customFileIndex )
    {
        this.WriteDataToFile( null, null, outputs );
    }

    /**
     * Write parsed Endomondo data to file, if the collection is not empty.
     * 
     * @param users
     * @param userWorkouts
     * @param workouts
     */
    public void WriteDataToFile(
            Collection<String> users, 
            Collection<String> userWorkouts, 
            Collection<String> workouts )
    {
        String userOutPath = this.BuildOutputPath( usersFileName );
        String userWorkoutOutPath = this.BuildOutputPath( userWorkoutsFileName );
        String workoutOutPath = this.BuildOutputPath( workoutsFileName );
        
        this.OutputToFile( userOutPath, users );
        this.OutputToFile( userWorkoutOutPath, userWorkouts );
        this.OutputToFile( workoutOutPath, workouts );
    }
    
    protected String BuildOutputPath( String outFileName )
    {
        return this.getOutFilePath() + outFileName + Constants.Period 
                + Utilities.getDateTimeString() 
                + EndomondoPostProcessor.outFileExtension;
    }
    
    @Override
    protected void InitializeWriteToFile() throws NotDirectoryException
    {
        super.InitializeWriteToFile();
        
        this.outFilePath = this.outFilePath + directoryName 
                + Utilities.getDateTimeString() + Constants.Slash;
        
        new File( this.outFilePath ).mkdir();
    }
}
