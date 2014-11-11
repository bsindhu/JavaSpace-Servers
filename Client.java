package JSpace;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * JSpace.Client is the client program of our JavaSpace impelementation, 
 * (i.e., JSpace). It provides a user application with JavaSpace read(), 
 * write(), and take() operations, each contacting distributed JSpace
 * servers through a UDP multicast.
 *
 * @author  Munehiro Fukuda
 * @version %I% %G%
 * @since   1.0
  */
public class Client {
    private final int READ = 0;
    private final int WRITE = 1;
    private final int TAKE = 2;
    private InetAddress group = null;
    private MulticastSocket mSocket = null; // a socket to multicast a packet
    private DatagramSocket dSocket = null;  // a socket to receive a UDP packet
    private int port = 0;

    /**
     * The constructor receives a group IP address and a port to multicast
     * a UDP message to distributed JSpace servers.
     *
     * @param multicast_group a group IP address that should be in a range of
     *        "224.0.0.0" - "239.255.255.255".
     * @param port a port that should be in a range of 5001 - 65535.
     */
    public Client( String multicast_group, int port ) {
	try {
	    group = InetAddress.getByName( multicast_group );
	    mSocket = new MulticastSocket( );     
	    dSocket = new DatagramSocket( port ); 
	} catch( IOException e ) {
	    e.printStackTrace( );
	    System.exit( -1 );
	}
	this.port = port;
	System.err.println( "JSpace.Client create: group = " + 
			    multicast_group + ", port = " + port );
    }

    /**
     * write() multicasts a given entry to distributed JSpace servers.
     * @param userEntry an entry to be multicast to distributed JSpace servers.
     */
    public void write( Entry userEntry ) {
	try {
	    Entry entry = userEntry.getBase( ); // extract only a base entry
	    entry.setOperation( WRITE );        // set a write operation  
	    // entry.print( );
	    byte[] buf = Entry.serialize( entry ); // serialize and multicast
	    DatagramPacket packet                  // this entry
		= new DatagramPacket( buf, buf.length, group, port );
	    mSocket.send( packet );
	} catch( IOException e ) {
	    e.printStackTrace( );
	    System.exit( -1 );
	}
    }

    /**
     * read() receives an entry from one of distributed JSpace servers, which
     * maintain a given entry in its hash table.
     *
     * @param userTemplate a template that receives a user-requested entry
     *                     from a remote JSpace server.
     */
    public void read( Entry userTemplate ) {
	Entry template = userTemplate.getBase( ); // extract only a base entry
	template.setOperation( READ );            // set a read operation
	common( userTemplate, template );         // read a requested entry
    }                                             

    /**
     * read() receives an entry from one of distributed JSpace servers, which
     * maintain a given entry in its hash table. This operation removes
     * the entry from the responding JSpace servers.
     *
     * @param userTemplate a template that receives a user-requested entry
     *                     from a remote JSpace server.
     */
    public void take( Entry userTemplate ) {
	Entry template = userTemplate.getBase( ); // extract only a base entry
	template.setOperation( TAKE );            // set a take operation
	common( userTemplate, template );         // take out a requested entry
    }

    /**
     * common() is called from read() and write(), multicasts userTemplate to
     * distributed JSpace servers, and receives the corresponding entry from
     * one of these servers.
     *
     * @param userTemplate an entry to be multicast to JSpace servers.
     * @param template     an entry to receive from one of JSpace servers.
     */
    private void common( Entry userTemplate, Entry template ) {
	try {
	    byte[] buf = Entry.serialize( template ); // serialize an entry
	    DatagramPacket packet 
		= new DatagramPacket( buf, buf.length, group, port );
	    mSocket.send( packet ); // multicast this entry to servers.
	    
	    buf = new byte[1024];
	    packet = new DatagramPacket( buf, buf.length );
	    dSocket.receive( packet ); // receive an entry from a server

	    template = Entry.deserialize( buf ); // deserialize it
	    // template.print( );

	    userTemplate.setValue( template.getValue( ) ); // sets an answer
	} catch( Exception e ) {
	    e.printStackTrace( );
	    System.exit( -1 );
	}
    }
}