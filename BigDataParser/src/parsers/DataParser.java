package parsers;

import java.util.*;

import utilities.Utilities;

/**
 * 
 * @author fqiao
 * 
 *         Note: the implementation ParseMany is the lowest atomic unit in terms
 *         of threading, as it is used in Runnable. ParseMany MUST BE THREAD SAFE!
 *
 *         A DataParser converts data of type T into data of type K
 * 
 *         TODO: Integrate with Hadoop.
 *
 * @param <T>
 *            Input data format
 * @param <K>
 *            Parsed data format
 */
public abstract class DataParser<T, E>
{
    protected int batchSize = 1000;
    protected int threadCount = 10;
    
    public DataParser( int batchSize, int threadCount )
    {
        
        if( batchSize > 0 )
        {
            this.batchSize = batchSize;
        }
        
        if( threadCount > 0 )
        {
            this.threadCount = threadCount;
        }
    }
    
	public abstract Collection<E> ParseMany( Collection<T> inputs );

    /**
     * Parse a collection of strings into the output format.
     * 
     * Divides up the input collection into collection.size / thread count, and
     * calls parseMany on each subcollections, on separate threads.
     * 
     * 1). Enable multithreaded parsing. Done
     * TODO: 2). Integrate with Hadoop.
     * 
     * @param inputs
     *            A collection of data of type <T>.
     * @return A collection of parsed data in of type <E>.
     */
	public Collection<E> ParseBatch( Collection<T> inputs )
	{
	    List<T> inputList = Utilities.CollectionToList( inputs );
	    ArrayList<E> outputs = null;
	    
	    int actualThreadCount = inputs.size() < threadCount ? inputs.size() : threadCount;
	    
	    // Create a list of threads.
	    // Also a collection keeping track of the runnables because dumbass Java doesn't
	    // have a straight forward way to manage thread callbacks or returns for
	    // the simplest scenarios.
	    Thread[] threadPool = new Thread[actualThreadCount];
	    ArrayList<ParseBatchRunnable> batchPool
	        = new ArrayList<ParseBatchRunnable>( actualThreadCount );
	    
	    int actualBatchSize = batchSize < inputs.size() ? batchSize : inputs.size();
	    
	    int itemsPerThread = actualBatchSize / actualThreadCount;
	    itemsPerThread = itemsPerThread * actualThreadCount < actualBatchSize ? itemsPerThread + 1 : itemsPerThread;
	    
	    // Partition the lists and feed them to the runnables and then the threads
	    for( int i = 0; i < actualThreadCount; i++ )
	    {
	        int startIdx = i * itemsPerThread;
	        int endIdx = ( ( i + 1 ) * itemsPerThread ) < inputs.size()
	                ? ( ( i + 1 ) * itemsPerThread ) : inputs.size();
	        
	        ParseBatchRunnable runnable
	            = new ParseBatchRunnable( inputList.subList( startIdx, endIdx ), this );
            batchPool.add( runnable );
	        
	        threadPool[i] = new Thread( runnable );
	        threadPool[i].start();
	    }
	    
	    Utilities.JoinAll( threadPool );
	    
	    // Compute output array size first to avoid resizing massive arrays.
	    int batchOutputSize = 0;
	    for( ParseBatchRunnable runnable : batchPool )
	    {
	        batchOutputSize += runnable.ParseManyResults == null 
	                ? 0 : runnable.ParseManyResults.size();
	    }
	    
	    outputs = new ArrayList<E>( batchOutputSize );
	    
        for( ParseBatchRunnable runnable : batchPool )
        {
            outputs.addAll( runnable.ParseManyResults );
        }
	    
	    return outputs;
	}
	
	/**
	 * @author fqiao
	 *         
	 *         A runnable for the threads in ParseBatch, because in Java there is no simpler
	 *         way to make a bunch of simple, identical threads and get some return value.
	 *
	 */
	protected class ParseBatchRunnable implements Runnable
	{
	    private Collection<T> inputs;
	    
	    private DataParser<T,E> parser;
	    
	    public Collection<E> ParseManyResults;
	    
	    public ParseBatchRunnable( Collection<T> inputs,  DataParser<T,E> parser )
	    {
	        this.inputs = inputs;
	        this.parser = parser;
	    }
	    
	    public void run()
	    {
	        ParseManyResults = parser.ParseMany( inputs );
	    }
	}
}
