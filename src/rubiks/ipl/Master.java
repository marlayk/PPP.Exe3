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
	LinkedList<Cube> jobs;
	LinkedList<ReceivePortIdentifier> slaves;
	Object syncJobs;
	int givenJobs = 0;
	Object syncSolution;
	int solutions = 0;
	int bound = 0;
	
	public Master(Ibis ibis, Cube cube, PortType masterToSlave, PortType slaveToMaster)
	{
		this.myIbis = ibis;
		this.cube = cube;
		this.masterToSlavePortType = masterToSlave;
		this.slaveToMasterPortType = slaveToMaster;
		this.cache = new CubeCache(cube.getSize());
		this.jobs = new LinkedList<Cube>();
		this.slaves = new LinkedList<ReceivePortIdentifier>();
	}
	
	public void Run()
	{
		//Init
		ReceivePort receive = null;
		
		try 
		{
			receive = myIbis.createReceivePort(slaveToMasterPortType, null, this);
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
			this.bound ++;
			/*
			 * Solve
			 */
            System.out.print(" " + bound);
            
            cube.setBound(bound);
            this.jobs.add(cube);
            /*
             * Execute the first iterations.
             */
            while ( jobs.peek().getTwists() < INITIAL_ITERATION )
            {
            	Cube c = jobs.poll();
            	
            	if ( c.isSolved() )
            	{
            		this.solutions++;
            	}
            	
            	if ( c.getTwists() < c.getBound() )
            	{
	            	Cube[] children = c.generateChildren(cache);
	            	for ( Cube cc : children )
	            	{
	            		jobs.add(cc);
	            	}
            	}
            }
            /*
             * Send jobs to the slaves.
             * When there are no slaves waiting, perform some iteration.
             */
            while ( ! jobs.isEmpty() )
            {
            	Cube next = jobs.poll();
            	if ( ! slaves.isEmpty() )
            	{
            		try
            		{
            		SendPort send = myIbis.createSendPort(masterToSlavePortType);
            		send.connect(slaves.poll());
            		
            		WriteMessage job = send.newMessage();
            		job.writeObject(next);
            		job.finish();
            		}
            		catch (ConnectionFailedException e)
            		{
            			System.err.println("Unable to connect with the slave: " + e.getMessage());
            			return;
            		}
            		catch (IOException e)
            		{
            			//TODO: why crash??
            			System.err.println("Unable send the job to the slave: " + e.getMessage());
            			return;	
            		}
            	}
            	else
            	{
            		if ( next.isSolved() )
            		{
            			synchronized (syncSolution) {
            				this.solutions++;
						}
            		}
            		
            		if ( next.getTwists() < next.getBound() )
            		{
            			Cube[] children = next.generateChildren(cache);
                    	for ( Cube cc : children )
                    	{
                    		jobs.add(cc);
                    	}
            		}
            	}
            }
            /*
             * Wait for all the jobs to terminate.
             */
            while ( this.givenJobs > 0 )
            {
            	try 
            	{
					wait();
				} 
            	catch (InterruptedException e) 
            	{
            		System.err.println("Interrupted: " + e.getMessage());
        			return;	
				}
            }
		}
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
    			//TODO: why crash??
    			System.err.println("Unable send the job to the slave: " + e.getMessage());
    			return;	
    		}
		}
        System.out.println();
        System.out.println("Solving cube possible in " + this.solutions + " ways of "
                + bound + " steps");	
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
				notifyAll();
			}
		}
		
		synchronized (slaves) {
			slaves.add(resultMessage.receivePort);
		}
	}
}
