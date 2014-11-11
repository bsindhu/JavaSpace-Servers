package JSpace;

import com.jcraft.jsch.*;
import java.io.*;
import java.net.*;
import java.util.*;

public class Server {
    /**
     * The main() function runs both at the user local and remote machines.
     *
     * @param args args[0] receives the configuration file name
     */
    public static void main( String[] args ) {
	if ( args.length == 1 )
	    new Server( args[0] ); // master server to run locally
	else if ( args.length == 0 )
	    new Server( );         // slave servers to run remotely 
	else {
	    // invalid arguments
	    System.err.println( 
                "usage: java -cp jsch-0.1.44.jar:. JSpace.Server config.txt" );
	    System.exit( -1 );
	}
    }
	
    /**
     * This is the constructor executed at a user-local machine.
     *
     * @param configFile a configuration file name. It must include:
     *                   user account (1st line); password (2nd line); 
     *                   and all remote host IP names (the rest).
     */
    public Server( String configFile ) {
	final int JschPort = 22;      // Jsch IP port

	// Read configFile to initialize username, password, and hosts
	BufferedReader fileReader = null;
	String username = null;
	String password = null;
	String multicast_group = null;
	int port = 0;
	ArrayList<String> hosts = new ArrayList<String>( );
	try {
	    // Open a configuration file
	    fileReader 
		= new BufferedReader( new InputStreamReader
				      ( new BufferedInputStream
					( new FileInputStream
					  ( new File( configFile ) ) ) ) );
	    // Read all the contents
	    // 1st line: username
	    username = fileReader.readLine( ); 
	    // 2nd line: password
	    password = fileReader.readLine( ); 
	    // 3rd line: multicast group such as 239.255.255.255
	    multicast_group = fileReader.readLine( ); 
	    // 4th line: IP port such as 50763
	    port = Integer.parseInt( fileReader.readLine( ) ); 
	    // the rest: remote host IPs
	    while ( fileReader.ready( ) )
		hosts.add( fileReader.readLine( ) ); 
	    fileReader.close( );
	} catch( Exception e ) {
	    e.printStackTrace( );
	    System.exit( -1 );
	}

	// A command to launch remotely:
	//          java -cp ./jsch-0.1.44.jar:. JSpace.Server
	String cur_dir = System.getProperty( "user.dir" );
	String command 
	    = "java -cp " + cur_dir + "/jsch-0.1.44.jar:" + cur_dir + 
	      " JSpace.Server";

	// establish an ssh2 connection to each remote machine and run
	// JSpace.Server there.
	Connection connections[] = new Connection[hosts.size( )];
	for ( int i = 0; i < hosts.size( ); i++ ) {
	    connections[i] = new Connection( username, password, 
					     hosts.get( i ), command );
	}

	// the main body of the master server
	new JSpace( connections, multicast_group, port );

	// close the ssh2 connection with each remote machine
	for ( int i = 0; i < hosts.size( ); i++ ) {
	    connections[i].close( );
	}
    }

    /**
     * This is the constructor executed at each remote machine.
     *
     */
    public Server( ) {
	// receive an ssh2 connection from a user-local master server.
	Connection connection = new Connection( );

	// the main body of the slave server
	new JSpace( connection );
    }
}
