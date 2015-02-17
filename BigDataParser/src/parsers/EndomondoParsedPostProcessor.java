package parsers;

import java.nio.file.NotDirectoryException;
import java.util.Collection;

import utilities.Constants;

public class EndomondoParsedPostProcessor extends EndomondoPostProcessor
{

    protected static String workoutsIndexFileName = "workouts.index";
    protected static String dataKeysFileName = "data.keys";

    public EndomondoParsedPostProcessor( PostProcessType type,
            String outFilePath ) throws NotDirectoryException
    {
        super( type, outFilePath );
    }

    /**
     * Write parsed Endomondo data to file, if the collection is not empty.
     * 
     * @param workouts
     */
    @Override
    public void WriteDataToFile( Collection<String> workouts, String fileIndex )
    {
        String workoutOutPath = this.getOutFilePath() + workoutsFileName + Constants.Period 
            + fileIndex
            + EndomondoPostProcessor.outFileExtension;
        
        PostProcessor.OutputToFile( workoutOutPath, workouts );
    }
    

    /**
     * Write parsed Endomondo data to file, if the collection is not empty.
     * 
     * @param users
     * @param userWorkouts
     * @param workOutFileIndex
     * @param workouts
     */
    public void WriteDataToFile(
            Collection<String> users, 
            Collection<String> userWorkouts, 
            Collection<String> workOutFileIndex, 
            Collection<String> dataKeys,
            Collection<String> workouts )
    {
        super.WriteDataToFile( users, userWorkouts, workouts );

        String workoutIndexOutPath = this.BuildOutputPath( workoutsIndexFileName );
        String dataKeysOutPath = this.BuildOutputPath( dataKeysFileName );

        PostProcessor.OutputToFile( workoutIndexOutPath, workOutFileIndex );
        PostProcessor.OutputToFile( dataKeysOutPath, dataKeys );
    }
    
    @Override
    protected String BuildOutputPath( String outFileName )
    {
        return this.getOutFilePath() + outFileName  + EndomondoPostProcessor.outFileExtension;
    }
}