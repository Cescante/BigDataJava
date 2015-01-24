package parsers;

import java.io.*;
import java.nio.file.NotDirectoryException;
import java.util.Collection;

import utilities.*;

public class EndomondoPostProcessor extends PostProcessor
{
    private static String outFileExtension = ".json";
    private static String usersFileName = "users";
    private static String userWorkoutsFileName = "user.workouts";
    private static String workoutsFileName = "workouts";
    private static String directoryName = "EndomondoData.";
    
    public EndomondoPostProcessor( PostProcessType type, String outFilePath ) 
            throws NotDirectoryException
    {
        super( type, outFilePath );
    }
    
    /**
     * Override of the PostProcessor WriteDataToFile, outputs a batch
     * of parsed workouts to file.
     * 
     * @param outputs
     */
    @Override
    public void WriteDataToFile( Collection<String> outputs )
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
        
        PostProcessor.OutputToFile( userOutPath, users );
        PostProcessor.OutputToFile( userWorkoutOutPath, userWorkouts );
        PostProcessor.OutputToFile( workoutOutPath, workouts );
    }
    
    public String BuildOutputPath( String outFileName )
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
