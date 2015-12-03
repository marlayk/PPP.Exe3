package rubiks.ipl;

import java.io.IOException;

import ibis.ipl.*;

/**
 * This class represents a slave.
 */
public class Slave {
	
	Ibis myIbis;
	IbisIdentifier master;
	PortType masterToSlavePortType;
	PortType slaveToMasterPortType;
	CubeCache cache;
	Cube currentCube;
	int myResult;
	
	
	public Slave(Ibis ibis, IbisIdentifier master, PortType masterToSlave, PortType slaveToMaster, int cubeSize)
	{
		this.master = master;
		this.myIbis = ibis;
		this.masterToSlavePortType = masterToSlave;
		this.slaveToMasterPortType = slaveToMaster;
		this.cache = new CubeCache(cubeSize);
		this.myResult = -1;
		this.currentCube = null;
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

		do
		{
			
			/*
			 * Send the result (it will be just a request on the first iteration) to the server.
			 */
			try 
			{
				 WriteMessage result = send.newMessage();
			     result.writeObject(new ResultMessage(myResult, receive.identifier()));
			     result.finish();
			}
			catch ( IOException e)
			{
				System.err.println("Unable to send the result: " + e.getMessage());
				return;
			}
			/*
			 * Wait for new jobs.
			 */
			try
			{
				System.err.println("Slave weits. . .");
				ReadMessage job = receive.receive();
		        this.currentCube = (Cube) job.readObject();
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
			
			if ( this.currentCube != null)
			{
				/*
				 * If there is a new job, solve it.
				 */
				this.myResult = solutions(currentCube, cache);
			}
		} while ( this.currentCube != null);
		
		try 
		{
			send.close();
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to close the send port: " + e.getMessage());
			return;
		}
		
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
