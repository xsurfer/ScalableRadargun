package org.radargun;

import java.nio.channels.SocketChannel;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/1/13 Time: 8:14 PM
 */
public class SlaveSocketChannel {

   private final int id;
   private final SocketChannel socketChannel;

   public SlaveSocketChannel(int id, SocketChannel socketChannel) {
      this.id = id;
      this.socketChannel = socketChannel;
   }

   public int getId() {
      return id;
   }

   public SocketChannel getSocketChannel() {
      return socketChannel;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SlaveSocketChannel that = (SlaveSocketChannel) o;

      if (id != that.id) return false;
      if (!socketChannel.equals(that.socketChannel)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + socketChannel.hashCode();
      return result;
   }

}
