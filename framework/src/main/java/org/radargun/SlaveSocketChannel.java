package org.radargun;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 8:14 PM
 */
public class SlaveSocketChannel {

    private final int id;
    private final SocketChannel socketChannel;


    public SlaveSocketChannel(int id, SocketChannel socketChannel){
        this.id = id;
        this.socketChannel = socketChannel;
    }

    public int getId(){
        return id;
    }

    public SocketChannel getSocketChannel(){
        return socketChannel;
    }

}
