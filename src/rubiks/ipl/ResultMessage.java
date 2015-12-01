package rubiks.ipl;

import ibis.ipl.ReceivePortIdentifier;

/**
 * @author Vittorio Massaro
 *
 * This class represent a message sent from the slave to the master.
 */
public class ResultMessage {
	/*
	 * Result of the computation.
	 */
	int result;
	/*
	 * Receive port.
	 */
	ReceivePortIdentifier receivePort;
	
	public ResultMessage(int result, ReceivePortIdentifier receivePort)
	{
		this.receivePort = receivePort;
		this.result = result;
	}
}
