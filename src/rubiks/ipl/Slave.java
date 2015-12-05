package rubiks.ipl;

import java.io.IOException;

import ibis.ipl.*;

/**
 * @author Vittorio Massaro
 * 
 * A slave able to execute some cubes in order to find a solution.
 */
public class Slave {
	/*
	 * Ibis global parameters.
	 */
	Ibis myIbis;
	IbisIdentifier master;
	PortType masterToSlavePortType;
	PortType slaveToMasterPortType;
	/*
	 * The cache used.
	 */
	CubeCache cache;
	
	/**
	 * Creates a new Slave.
	 * 
	 * @param ibis
	 * 		The ibis identifier.
	 * @param master
	 * 		The master ibis identifier.
	 * @param masterToSlave
	 * 		The master-to-slave port type.
	 * @param slaveToMaster
	 * 		The slave-to-master port type.
	 * @param cubeSize
	 * 		The size of the cube to be solved.
	 */
	public Slave(Ibis ibis, IbisIdentifier master, PortType masterToSlave, PortType slaveToMaster, int cubeSize)
	{
		this.master = master;
		this.myIbis = ibis;
		this.masterToSlavePortType = masterToSlave;
		this.slaveToMasterPortType = slaveToMaster;
		this.cache = new CubeCache(cubeSize);
	}
	
	public void Run()
	{
		/*
		 * Create a sendPort and connect.
		 */		
		SendPort send = null;
		try 
		{
			send = myIbis.createSendPort(slaveToMasterPortType);
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to create the send port: " + e.getMessage());
			return;
		}
		try 
		{
			send.connect(master, "slave-to-master");
		} 
		catch (ConnectionFailedException e) 
		{
			System.err.println("Unable to connect to the master: " + e.getMessage());
			return;
		}
		
		/*
		 * Create the receive port for the job.
		 */
		ReceivePort receive = null;
		try 
		{
			receive = myIbis.createReceivePort(masterToSlavePortType, null);
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to create the receive port: " + e.getMessage());
			return;
		}
		receive.enableConnections();
		/*
		 * Send a message to the server, asking for jobs.
		 */
		try 
		{
			 WriteMessage result = send.newMessage();
		     result.writeObject(receive.identifier());
		     result.finish();
		}
		catch ( IOException e)
		{
			System.err.println("Unable to send the result: " + e.getMessage());
			return;
		}
		/*
		 * The cubes to solve will be in this variable.
		 */
		Cube[] currentCubes = null;
		do
		{
			/*
			 * Wait for new jobs.
			 */
			try
			{
				ReadMessage job = receive.receive();
				currentCubes = (Cube[]) job.readObject();
		        job.finish();
			}
			catch ( IOException e)
			{
				System.err.println("Unable to receive the job: " + e.getMessage());
				return;
			}
			catch ( ClassNotFoundException e)
			{
				System.err.println("Unable to read the Cube: " + e.getMessage());
				return;
			}
			
			if ( currentCubes != null)
			{
				/*
				 * If there is a new job, solve it.
				 */
				int solutions = 0;
				for ( Cube currentCube : currentCubes)
				{
					solutions += solutions(currentCube, cache);
				}
				/*
				 * Send the result back.
				 */
				try 
				{
					 WriteMessage result = send.newMessage();
				     result.writeInt(solutions);
				     result.finish();
				}
				catch ( IOException e)
				{
					System.err.println("Unable to send the result: " + e.getMessage());
					return;
				}
			}
		} while ( currentCubes != null);
		/*
		 * Close the sent port.
		 */
		try 
		{
			send.close();
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to close the send port: " + e.getMessage());
			return;
		}
		/*
		 * Close the receive port.
		 */
		try 
		{
			receive.close();
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to close the receive port: " + e.getMessage());
			return;
		}
	}
	
	/**
     * Recursive function to find a solution for a given cube. Only searches to
     * the bound set in the cube object.
     * 
     * @param cube
     *            cube to solve
     * @param cache
     *            cache of cubes used for new cube objects
     * @return the number of solutions found
     */
    private static int solutions(Cube cube, CubeCache cache) {
        if (cube.isSolved()) {
            return 1;
        }

        if (cube.getTwists() >= cube.getBound()) {
            return 0;
        }

        // generate all possible cubes from this one by twisting it in
        // every possible way. Gets new objects from the cache
        Cube[] children = cube.generateChildren(cache);

        int result = 0;

        for (Cube child : children) {
            // recursion step
            int childSolutions = solutions(child, cache);
            if (childSolutions > 0) {
                result += childSolutions;
            }
            // put child object in cache
            cache.put(child);
        }
        return result;
    }
}
