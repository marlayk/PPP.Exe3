package rubiks.bonus;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import ibis.ipl.*;

/**
 * @author Vittorio Massaro
 * 
 * A master able to solve the Rubik's Cube puzzle.
 * The master creates new jobs, and distributes them in a balanced way among the slaves.
 */
public class Master{
	/*
	 * The INITIAL_TWISTS specify how many different sizes of cubes there will
	 * be in the initial work queue for each bound.
	 */
	static final int INITIAL_TWISTS = 3;
	/*
	 * Jobs that needs less than SEQUENTIAL_THRESHOLD twists are not even sent to slaves.
	 */
	static final int SEQUENTIAL_THRESHOLD = 2;
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
	 * Thread pool
	 */
	ExecutorService executor;
	Queue<Future<Integer>> results;
	/*
	 * Receive and send ports.
	 */
	ReceivePort receive = null;
	LinkedList<SendPort> sendPorts = new LinkedList<SendPort>();
	/*
	 * jobs represents the jobs stack.
	 * auxQueue is an auxiliary queue used for the job creation and distribution.
	 * It's declared as a field in order to avoid to allocate it for each iteration.
	 */
	LinkedList<Cube> jobs = new LinkedList<Cube>();
	LinkedList<Cube> auxQueue = new LinkedList<Cube>();
	/*
	 * Variables used during the solution of the cube.
	 * They indicate the current bound, the number of solution found and the number
	 * of jobs that slaves that are idle. 
	 */
	int bound = 0;
	int solutions = 0;
	int slavesAvailable = 0;
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
		this.executor = Executors.newCachedThreadPool();
		this.results = new LinkedList<Future<Integer>>();
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
		 * Wait for all the saves to be ready, and initialize send ports.
		 */
		waitForSlaves();
		
		/*
		 * Solve and take the duration time.
		 */
		long start = System.currentTimeMillis();
		this.Solve();
		long end = System.currentTimeMillis();
		/*
		 * Print the duration timer.
		 */
		System.err.println("Solving cube took " + (end - start) + " milliseconds");
		/*
		 * Quit the pool
		 */
		this.executor.shutdown();
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
		
		System.out.print("Bound now:");
		/*
		 * The cube is already solved.
		 * In this case the sequential version says that the cube is solvable in 1 way with one step.
		 * I'm not sure if this is a bug of something, but I'll do the same.
		 */
		if ( cube.isSolved() )
		{
			this.solutions = 1;
			this.bound = 1;
            System.out.print(" 1");
    		System.out.println();
            System.out.println("Solving cube possible in " + this.solutions + " ways of " + bound + " steps");	
            return;
		}
		
		while ( this.solutions == 0 )
		{
			/*
			 * Increase the bound and print it.
			 */
			this.bound ++;
            System.out.print(" " + bound);
            /*
             * Set the bound and put the new cube in the jobs queue.
             */
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
            while ( !jobs.isEmpty() )
            {
            	Cube c = jobs.pop();
            	/*
            	 * Solve your jobs.
            	 */
            	if ( c.getTwists() > 1 ) {
            		this.results.add(this.executor.submit(new solverThread(c)));
            	}
            	else {
					for ( Cube cc : c.generateChildren(cache))
					{
						this.results.add(this.executor.submit(new solverThread(cc)));
					}
				}
            }
			/*
			 * Read results.
			 */
			while ( !this.results.isEmpty() )
			{
				try {
					solutions += results.poll().get();
				} 
				catch (InterruptedException e) {
					System.err.println("Waiting for the results in slave: " + e.getMessage());
					return;
				} catch (ExecutionException e) {
					System.err.println("Waiting for the results in slave: " + e.getMessage());
					return;
				}
			}
            /*
             * Wait for all the slaves to terminate their jobs.
             */
            while ( this.slavesAvailable < this.slavesN )
            {
            	try
            	{
            		/*
            		 * Of course, the slaves will also send the number of solution they found.
            		 */
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
	 * Sends the given cube array to the indicated port.
	 * 
	 * @param port
	 * 			The port identifier of the indicated receive port.
	 * @param cubes
	 * 			The cube array to be sent.
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
    		 * Decrease the number of idle slaves.
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
            	 * Put it in the list.
            	 */
            	this.sendPorts.add(sendPort);
            	/*
            	 * Indicate that the message can be re-used.
            	 */
            	readMessage.finish();
            	/*
            	 * Increase number of idle slaves.
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
			 * Close the sending ports.
			 */
			for ( SendPort port : sendPorts)
			{
				port.close();
			}
			/*
			 * Close receive port.
			 */
			receive.close();
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
		 * I want to have in the job queue cubes with INITIAL_TWISTS different number of twists.
		 * Of course, if the bound is less than that number, I can't execute more twists than the bound value.
		 */
		int initial_twists = INITIAL_TWISTS;
        for(int i = 0; i <  Math.min(initial_twists, this.bound); i++)
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
        	/*
        	 * I want at least INITIAL_TWISTS different twists in my jobs queue.
        	 * So, if all the jobs are going to be twisted again, I'll need an extra iteration.
        	 */
        	if ( jobs.size()  < poolSize ) initial_twists++;
        }
	}
	/**
	 * Sends jobs to the slaves
	 */
	private void distributeJobs()
	{
		/*
		 * If the first (so the heaviest) job needs less then SEQUENTIAL_THRESHOLD twists, jobs are not distributed.
		 */
		//if ( jobs.peek().getBound() - jobs.peek().getTwists() < SEQUENTIAL_THRESHOLD ) return;			
		
		int maxJob = (int)Math.ceil(jobs.size()/(slavesN+1));
		Cube[][] distributedJobs = new Cube[slavesN][maxJob];
		/*
		 * Jobs are distributed in a round robin fashion (this is necessary, since the jobs tend to be less heavy
		 * while going to the end of the queue.
		 */
		for (int i = 0; i < maxJob; i++)
		{
			auxQueue.add(jobs.pop());
			for ( int j = 0; j < slavesN; j++ )
			{
				if ( !jobs.isEmpty())
				{
					distributedJobs[j][i] = jobs.pop();
				}
			}
		}
		/*
		 * Jobs are sent.
		 */
		for ( int i = 0; i < slavesN; i++)
		{
			send(sendPorts.get(i), distributedJobs[i]);
		}
		/*
		 * Jobs that are going to be executed by the master are put in the jobs queue.
		 */
		while ( !auxQueue.isEmpty() )
		{
			jobs.add(auxQueue.pop());
		}
	}
}
