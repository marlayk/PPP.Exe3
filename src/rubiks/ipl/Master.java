package rubiks.ipl;

import java.io.IOException;
import java.util.LinkedList;

import ibis.ipl.*;

/**
 * This class represents a master.
 */
public class Master{
	
	Ibis myIbis;
	Cube cube;
	PortType masterToSlavePortType;
	PortType slaveToMasterPortType;
	CubeCache cache;
	ReceivePort receive = null;
	LinkedList<ReceivePortIdentifier> slaves;
	int givenJobs = 0;
	int solutions = 0;
	int bound = 0;
	
	public Master(Ibis ibis, Cube cube, PortType masterToSlave, PortType slaveToMaster)
	{
		this.myIbis = ibis;
		this.cube = cube;
		this.masterToSlavePortType = masterToSlave;
		this.slaveToMasterPortType = slaveToMaster;
		this.cache = new CubeCache(cube.getSize());
		this.slaves = new LinkedList<ReceivePortIdentifier>();
	}
	
	public void Run()
	{
		//Init
		
		try 
		{
			receive = myIbis.createReceivePort(slaveToMasterPortType, "slave-to-master");
			receive.enableConnections();
		} 
		catch (IOException e)
		{
			System.err.println("Unable to create the receive port: " + e.getMessage());
			return;
		}
		// solve
		long start = System.currentTimeMillis();
		this.Solve();
		long end = System.currentTimeMillis();
		System.err.println("Solving cube took " + (end - start) + " milliseconds");
		/*
		 * Quit slaves.
		 */
		for ( ReceivePortIdentifier slave : slaves)
		{
			sendCube(slave, null);
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

	private void Solve() {
		/*
		 * 
		 */
		this.bound = 0;
		this.solutions = 0;
		
		while ( this.solutions == 0 )
		{
			/*
			 * Solve
			 */
			this.bound ++;
            System.out.print(" " + bound);
            
            cube.setBound(bound);
            int tmpSolutions = solutions(cube, cache);
            
            this.solutions += tmpSolutions;
            
            /*
             * Wait for all the jobs to terminate.
             */
            while ( this.givenJobs > 0 )
            {
            	try
            	{
	            	ReadMessage result = receive.receive();
	            	ResultMessage message = (ResultMessage) result.readObject();
	            	if ( message.result != -1)
	        		{
	        			this.solutions += message.result;
	        			this.givenJobs--;
	        		}
	            	this.slaves.add(message.receivePort);
	            	result.finish();
            	}
            	catch (ClassNotFoundException e1) 
				{
					System.err.println("During result.readObject(): " + e1.getMessage());
				} 
				catch (IOException e1) 
				{
					System.err.println("During result.readObject() or receive.receive(): " + e1.getMessage());
				}
            }
		}
		System.out.println();
        System.out.println("Solving cube possible in " + this.solutions + " ways of "
                + bound + " steps");	
	}

	private int solutions(Cube cube, CubeCache cache) {
        if (cube.isSolved()) {
            return 1;
        }

        if (cube.getTwists() >= cube.getBound()) {
            return 0;
        }
        //TODO: Giocare qui.
        if ( !(cube.getTwists() < 2) && !(cube.getBound() - cube.getTwists() < 4) )
    	{
        	/*
        	 * check
        	 */
        	if ( !this.slaves.isEmpty() )
        	{
        		sendCube(this.slaves.poll(), cube);
        		return 0;
        	}
        	
        	ReadMessage result = null;
			try 
			{
				result = receive.poll();
			} 
			catch (IOException e1) 
			{
				System.err.println("During receive.poll(): " + e1.getMessage());
			}
        	if ( result != null)
        	{
        		ResultMessage message = null;
				try 
				{
					message = (ResultMessage) result.readObject();
				} 
				catch (ClassNotFoundException e1) 
				{
					System.err.println("During result.readObject(): " + e1.getMessage());
				} 
				catch (IOException e1) 
				{
					System.err.println("During result.readObject(): " + e1.getMessage());
				}
        		
        		if ( message.result != -1)
        		{
        			this.solutions += message.result;
        			this.givenJobs--;
        		}
        		
	    		sendCube(message.receivePort,cube);
	    		
	    		try 
	    		{
					result.finish();
				} 
	    		catch (IOException e) 
	    		{
					System.err.println("Exception during result.finish(): " + e.getMessage());
				}
	    		return 0;
        	}
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
	
	public void sendCube (ReceivePortIdentifier port,  Cube cube)
	{
		try
		{
    		SendPort send = myIbis.createSendPort(masterToSlavePortType);
    		send.connect(port);
    		
    		WriteMessage job = send.newMessage();
    		job.writeObject(cube);
    		job.finish();
    		
    		send.close();
    		
    		this.givenJobs++;
		}
		catch (ConnectionFailedException e)
		{
			System.err.println("Unable to connect with the slave: " + e.getMessage());
			return;
		}
		catch (IOException e)
		{
			System.err.println("Unable send the job to the slave: " + e.getMessage());
			return;	
		}
	}
}
