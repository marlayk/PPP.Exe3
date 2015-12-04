package rubiks.ipl;

import java.io.IOException;
import java.util.LinkedList;

import ibis.ipl.*;

/**
 * This class represents a master.
 */
public class Master{
	static final int INITIAL_TWISTS = 2;
	static final int SEQUENTIAL_THRESHOLD = 3;
	/*
	 * Ibis global parameters.
	 */
	Ibis myIbis;
	PortType masterToSlavePortType;
	PortType slaveToMasterPortType;
	/*
	 * The cube to solve.
	 */
	Cube cube;
	/*
	 * The cache used.
	 */
	CubeCache cache;
	/*
	 * Receive and send ports.
	 */
	ReceivePort receive = null;
	LinkedList<SendPort> sendPorts = new LinkedList<SendPort>();
	/*
	 * The jobs stack.
	 */
	LinkedList<Cube> jobs = new LinkedList<Cube>();
	LinkedList<Cube> auxQueue = new LinkedList<Cube>();
	/*
	 * Variables used during the solution of the cube.
	 * They indicate the current bound, the number of solution found and the number
	 * of jobs that slaves that are idle. 
	 */
	int slavesAvailable = 0;
	int solutions = 0;
	int bound = 0;
	/*
	 * The number of slaves in the pool.
	 */
	int slavesN;
	/**
	 * Creates a new Master.
	 * 
	 * @param ibis
	 * 		The ibis identifier.
	 * @param cube
	 * 		The cube to be solved.
	 * @param masterToSlave
	 * 		The master-to-slave port type.
	 * @param slaveToMaster
	 * 		The slave-to-master port type.
	 */
	public Master(Ibis ibis, Cube cube, PortType masterToSlave, PortType slaveToMaster)
	{
		/*
		 * Copy the parameters in the object fields.
		 */
		this.myIbis = ibis;
		this.cube = cube;
		this.masterToSlavePortType = masterToSlave;
		this.slaveToMasterPortType = slaveToMaster;
		/*
		 * Initialization of the local cache.
		 */
		this.cache = new CubeCache(cube.getSize());
		/*
		 * The number of slaves is the size of the pool, minus the master.
		 */
		slavesN = myIbis.registry().getPoolSize() - 1;
	}
	public void Run()
	{
		/*
		 * Initialization of the receive port.
		 */
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
		
		/*
		 * Wait for all the saves to be ready.
		 */
		waitForSlaves();
		
		/*
		 * Solve and take the timestamp.
		 */
		long start = System.currentTimeMillis();
		this.Solve();
		long end = System.currentTimeMillis();
		System.err.println("Solving cube took " + (end - start) + " milliseconds");
		
		/*
		 * Quit slaves.
		 */
		quitSlaves();
		/*
		 * Close the send and receive ports.
		 */
		closePorts();
	}
	/**
	 * 
	 * Solves the current cube.
	 */
	private void Solve() {
		
		while ( this.solutions == 0 )
		{
			/*
			 * Solve
			 */
			this.bound ++;
            System.out.print(" " + bound);
            
            cube.setBound(bound);
            jobs.add(cube);
            /*
             * Generate jobs.
             */
            generateJobs();
            /*
             * Distribute Jobs
             */
            distributeJobs();
            /*
             * Solve.
             */
            while ( !jobs.isEmpty() )
            {
            	/*
            	 * Solve.
            	 */
            	this.solutions += solutions(jobs.pop(), cache);
            }
            /*
             * Wait for all the jobs to terminate.
             */
            while ( this.slavesAvailable < this.slavesN )
            {
            	try
            	{
	            	ReadMessage result = receive.receive();
        			this.solutions += result.readInt();
        			this.slavesAvailable++;
	            	result.finish();
            	}
				catch (IOException e1) 
				{
					System.err.println("During result.readObject() or receive.receive(): " + e1.getMessage());
				}
            }
		}
		System.out.println();
        System.out.println("Solving cube possible in " + this.solutions + " ways of " + bound + " steps");	
	}
	/**
     * Recursive function to find a solution for a given cube. Only searches to
     * the bound set in the cube object.
     * Some cubes are sent so salves.
     * 
     * @param cube
     *            cube to solve
     * @param cache
     *            cache of cubes used for new cube objects
     * @return the number of solutions found locally.
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
	/**
	 * Sends the given cube to the indicated port.
	 * 
	 * @param port
	 * 			The port identifier of the indicated receive port.
	 * @param cube
	 * 			The cube to be sent.
	 */			
	private void send (SendPort sendPort,  Cube[] cubes)
	{
		try
		{
    		/*
    		 * Create a new message.
    		 */
    		WriteMessage writeMessage = sendPort.newMessage();
    		/*
    		 * Write the cube to send in the message.
    		 */
    		writeMessage.writeObject(cubes);
    		/*
    		 * Send the message.
    		 */
    		writeMessage.finish();
    		/*
    		 * Increase the number of active jobs.
    		 */
    		this.slavesAvailable--;
		}
		catch (IOException e)
		{
			System.err.println("Unable send the job to the slave: " + e.getMessage());
			return;	
		}
	}
	/**
	 * This method waits for all the slaves to send a message to the master.
	 * The send ports needed for the communication are allocated.
	 */
	private void waitForSlaves()
	{
		
		while ( this.slavesAvailable < slavesN)
		{
			try
        	{
				/*
				 * Read the message.
				 */
            	ReadMessage readMessage = receive.receive();
            	/*
            	 * Read the object.
            	 */
            	ReceivePortIdentifier receivePortID = (ReceivePortIdentifier) readMessage.readObject();
            	/*
            	 * Create a new SendPort for the given slave.
            	 */
            	SendPort sendPort = myIbis.createSendPort(masterToSlavePortType);
            	/*
            	 * Connect it.
            	 */
            	sendPort.connect(receivePortID);
            	/*
            	 * Put it in the map.
            	 */
            	this.sendPorts.add(sendPort);
            	/*
            	 * Indicate that the message can be re-used.
            	 */
            	readMessage.finish();
            	/*
            	 * Increase num of slaves.
            	 */
            	this.slavesAvailable++;
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
	/**
	 * This method quits all the slaves, sending them a null message.
	 */
	private void quitSlaves()
	{
		for ( SendPort sendPort : sendPorts)
		{
			send(sendPort, null);
		}
	}
	/**
	 * This method closed both send and receive ports.
	 */
	private void closePorts()
	{
		try 
		{
			/*
			 * Close receive port.
			 */
			receive.close();
			/*
			 * Iterate on sending ports.
			 */
			for ( SendPort port : sendPorts)
			{
				port.close();
			}
		} 
		catch (IOException e) 
		{
			System.err.println("Unable to close the ports: " + e.getMessage());
			return;
		}
	}
	/**
	 * Generates the jobs for the current iteration.
	 */
	private void generateJobs()
	{
		int poolSize = slavesN + 1;
		/*
		 * I want to perform at most INITIAL_TWISTS twists.
		 */
        for(int i = 0; i <  Math.min(INITIAL_TWISTS, this.bound); i++)
        {
        	/*
        	 * If the jobs can be distributed in balanced way, stop.
        	 */
        	if ( jobs.size() % poolSize == 0) break;
        	/*
        	 * Make another step in jobs that can't be distributed in balanced way.
        	 */
        	int modulo = jobs.size() % poolSize;
        	for ( int j = 0; j < modulo; j++)
        	{
        		auxQueue.push(jobs.pollLast());
        	}
        	while (! auxQueue.isEmpty())
        	{
            	Cube c = auxQueue.pop();
            	Cube[] child = c.generateChildren(cache);
            	for ( Cube ch : child)
            	{
            		jobs.add(ch);
            	}
        	}
        }
        
	}
	/**
	 * Sends jobs to the slaves
	 */
	private void distributeJobs()
	{
		int maxJob = (int)Math.ceil(jobs.size()/(slavesN+1));
		Cube[][] distributedJobs = new Cube[slavesN][maxJob];
		for (int i = 0; i < maxJob; i++)
		{
			for ( int j = 0; j < slavesN; j++ )
			{
				if ( !jobs.isEmpty())
				{
					distributedJobs[j][i] = jobs.pop();
				}
			}
			auxQueue.add(jobs.pop());
		}
		
		for ( int i = 0; i < slavesN; i++)
		{
			send(sendPorts.get(i), distributedJobs[i]);
		}
		
		while ( !auxQueue.isEmpty() )
		{
			jobs.add(auxQueue.pop());
		}
	}
}
