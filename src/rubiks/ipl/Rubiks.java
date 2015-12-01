package rubiks.ipl;

import java.io.IOException;


import ibis.ipl.*;

/**
 * Parallel solver for rubik's cube puzzle.
 * 
 */

public class Rubiks {

	public static final boolean PRINT_SOLUTION = false;
	/*
	 * 	Port Types.
	 */
	static PortType masterToSlavePortType = new PortType(PortType.CONNECTION_ONE_TO_ONE, PortType.COMMUNICATION_RELIABLE, 
			PortType.RECEIVE_EXPLICIT, PortType.SERIALIZATION_OBJECT);
	
	static PortType slaveToMasterPortType = new PortType(PortType.CONNECTION_MANY_TO_ONE, PortType.COMMUNICATION_RELIABLE, 
					PortType.RECEIVE_AUTO_UPCALLS, PortType.SERIALIZATION_OBJECT);
	
	static IbisCapabilities ibisCapabilities = new IbisCapabilities(IbisCapabilities.ELECTIONS_STRICT);
	
	public static void printUsage() {
		System.out.println("Rubiks Cube solver");
		System.out.println("");
		System.out
			.println("Does a number of random twists, then solves the rubiks cube with a simple");
		System.out
			.println(" brute-force approach. Can also take a file as input");
		System.out.println("");
		System.out.println("USAGE: Rubiks [OPTIONS]");
		System.out.println("");
		System.out.println("Options:");
		System.out.println("--size SIZE\t\tSize of cube (default: 3)");
		System.out
			.println("--twists TWISTS\t\tNumber of random twists (default: 11)");
		System.out
			.println("--seed SEED\t\tSeed of random generator (default: 0");
		System.out
			.println("--threads THREADS\t\tNumber of threads to use (default: 1, other values not supported by sequential version)");
		System.out.println("");
		System.out
			.println("--file FILE_NAME\t\tLoad cube from given file instead of generating it");
		System.out.println("");
	}

	/**
	* Main function.
	* 
	* @param arguments
	*            list of arguments
	*/
	public static void main(String[] arguments) {
		Cube cube = null;

		// default parameters of puzzle
		int size = 3;
		int twists = 11;
		int seed = 0;
		String fileName = null;

		// number of threads used to solve puzzle
		// (not used in sequential version)

		for (int i = 0; i < arguments.length; i++) {
		    if (arguments[i].equalsIgnoreCase("--size")) {
			i++;
			size = Integer.parseInt(arguments[i]);
		    } else if (arguments[i].equalsIgnoreCase("--twists")) {
			i++;
			twists = Integer.parseInt(arguments[i]);
		    } else if (arguments[i].equalsIgnoreCase("--seed")) {
			i++;
			seed = Integer.parseInt(arguments[i]);
		    } else if (arguments[i].equalsIgnoreCase("--file")) {
			i++;
			fileName = arguments[i];
		    } else if (arguments[i].equalsIgnoreCase("--help") || arguments[i].equalsIgnoreCase("-h")) {
			printUsage();
			System.exit(0);
		    } else {
			System.err.println("unknown option : " + arguments[i]);
			printUsage();
			System.exit(1);
		    }
		}

		// create cube
		if (fileName == null) {
		    cube = new Cube(size, twists, seed);
		} else {
		    try {
			cube = new Cube(fileName);
		    } catch (Exception e) {
			System.err.println("Cannot load cube from file: " + e);
			System.exit(1);
		    }
		}
		
		//Initialization.
		Ibis ibis = null;
		try
		{
			ibis = IbisFactory.createIbis(ibisCapabilities, null, slaveToMasterPortType, masterToSlavePortType);
		}
		catch (IbisCreationFailedException e)
		{
			System.err.println("Ibis creation failed: " + e.getMessage());
			System.exit(1);
		}
		//Master election.
		IbisIdentifier master = null;
		try 
		{
			master = ibis.registry().elect("master");
		} 
		catch (IOException e) 
		{
			System.err.println("Master election failed: " + e.getMessage());
			System.exit(1);
		}
		
		if ( master.equals(ibis.identifier()) )
		{
			// print cube info.
			System.out.println("Searching for solution for cube of size "
				+ cube.getSize() + ", twists = " + twists + ", seed = " + seed);
			cube.print(System.out);
			System.out.flush();	
			//Start the master.
			new Master(ibis, cube, masterToSlavePortType, slaveToMasterPortType).Run();
		}
		else
		{
			new Slave(ibis, master, masterToSlavePortType, slaveToMasterPortType, cube.getSize()).Run();
		}
		
		try 
		{
			ibis.end();
		} 
		catch (IOException e) 
		{
			System.err.println("Termination failed: " + e.getMessage());
			System.exit(1);
		}
	}
}
