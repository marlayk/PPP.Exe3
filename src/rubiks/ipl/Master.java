package rubiks.ipl;

import java.io.IOException;
import java.util.LinkedList;

import ibis.ipl.*;

/**
 * This class represents a master.
 */
public class Master implements MessageUpcall{
	
	static final int INITIAL_ITERATION = 2;
	
	Ibis myIbis;
	Cube cube;
	PortType masterToSlavePortType;
	PortType slaveToMasterPortType;
	CubeCache cache;
	LinkedList<ReceivePortIdentifier> slaves;
	Object syncJobs = new Object();
	Object monitor = new Object();
	int givenJobs = 0;
	Object syncSolution = new Object();
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
		ReceivePort receive = null;
		
		try 
		{
			receive = myIbis.createReceivePort(slaveToMasterPortType, "slave-to-master", this);
			receive.enableConnections();
			receive.enableMessageUpcalls();
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
		 * Quit all the slaves.
		 */
		for ( ReceivePortIdentifier slave : slaves)
		{
			try
    		{
	    		SendPort send = myIbis.createSendPort(masterToSlavePortType);
	    		send.connect(slave);
	    		
	    		WriteMessage job = send.newMessage();
	    		job.writeObject(null);
	    		job.finish();
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
            
            //DEBUG
            /*try {
				System.in.read();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
            
            cube.setBound(bound);
            int tmpSolutions = solutions(cube, cache);
            
            synchronized (syncSolution) {
            	this.solutions += tmpSolutions;
			}
            
            /*
             * Wait for all the jobs to terminate.
             */
            System.err.println("Wait termination. . .");
            while ( this.givenJobs > 0 )
            {
            	/*try 
            	{
					//monitor.wait();
				} 
            	catch (InterruptedException e) 
            	{
            		System.err.println("Interrupted: " + e.getMessage());
        			return;	
				}*/
            }
            System.err.println("Termination. . .");
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
        
        if ( ! slaves.isEmpty() &&  ! (cube.getTwists() < INITIAL_ITERATION) && !(cube.getBound() - cube.getTwists() < 3))
    	{
    		sendCube(cube);
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
	
	public void sendCube (Cube cube)
	{
		try
		{
    		SendPort send = myIbis.createSendPort(masterToSlavePortType);
    		send.connect(slaves.poll());
    		
    		WriteMessage job = send.newMessage();
    		job.writeObject(cube);
    		job.finish();
    		synchronized (syncJobs) 
    		{
				this.givenJobs++;
			}
    		
    		send.close();
		}
		catch (ConnectionFailedException e)
		{
			/*
			 * TODO: handle this.
			 */
			System.err.println("Unable to connect with the slave: " + e.getMessage());
			return;
		}
		catch (IOException e)
		{
			System.err.println("Unable send the job to the slave: " + e.getMessage());
			return;	
		}
	}
	
	@Override
	public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
		
		/*
		 * 
		 */
		ResultMessage resultMessage = (ResultMessage) message.readObject();
		message.finish();
		
		if ( resultMessage.result > 0 )
		{
			synchronized (syncSolution) {
				this.solutions += resultMessage.result;
			}
		}
		
		if ( resultMessage.result != -1 )
		{
			synchronized (syncJobs) {
				givenJobs --;
			}
		}
		
		synchronized (slaves) {
			slaves.add(resultMessage.receivePort);
		}
	}
}
