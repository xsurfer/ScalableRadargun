package org.radargun.portings.microbenchmark.domain;

import org.radargun.CacheWrapper;

import java.io.Serializable;
import java.util.UUID;

public class TreeMapJvstm<K extends Comparable<? super K>, V> {

   private static enum Color {RED, BLACK}

   ;

   private Node myNilNode;

   private Node getRoot(CacheWrapper wrapper) {
      return ((Node) Micro.get(wrapper, "root"));
   }

   private void setRoot(CacheWrapper wrapper, Node root) {
      Micro.put(wrapper, "root", root);
   }

   public TreeMapJvstm(CacheWrapper cache, K ignoreKey, V ignoreValue) {
      this.myNilNode = new Node(cache, ignoreKey, ignoreValue, Color.BLACK, true);
      setRoot(cache, this.myNilNode);
   }

   public boolean containsKey(CacheWrapper wrapper, K key) {
      return !getNode(wrapper, key).isNil();
   }

   public V get(CacheWrapper wrapper, K key) {
      return getNode(wrapper, key).getValue(wrapper);
   }

   public V put(CacheWrapper wrapper, K key, V value) {
      Node current = getRoot(wrapper);
      Node parent = myNilNode;
      int comparison = 0;

      // Find new node's parent.
      while (!current.isNil()) {
         parent = current;
         comparison = key.compareTo(current.getKey(wrapper));
         if (comparison > 0)
            current = current.getRight(wrapper);
         else if (comparison < 0)
            current = current.getLeft(wrapper);
         else {
            // Key already in tree.
            V ret = current.getValue(wrapper);
            current.setValue(wrapper, value);
            return ret;
         }
      }

      // Set up new node.
      Node n = new Node(wrapper, key, value, Color.RED);
      n.setParent(wrapper, parent);

      // Insert node in tree.
      if (parent.isNil()) {
         // Special case inserting into an empty tree.
         setRoot(wrapper, n);
         return null;
      }
      if (comparison > 0)
         parent.setRight(wrapper, n);
      else
         parent.setLeft(wrapper, n);

      // Rebalance after insert.
      insertFixup(wrapper, n);
      return null;
   }

   public V remove(CacheWrapper wrapper, K key) {
      Node n = getNode(wrapper, key);
      if (n.isNil())
         return null;
      // Note: removeNode can alter the contents of n, so save value now.
      V result = n.getValue(wrapper);
      removeNode(wrapper, n);
      return result;
   }

   private void deleteFixup(CacheWrapper wrapper, Node node, Node parent) {
      while (node != getRoot(wrapper) && node.getColor(wrapper) == Color.BLACK) {
         if (node == parent.getLeft(wrapper)) {
            // Rebalance left side.
            Node sibling = parent.getRight(wrapper);
            // if (sibling == nil)
            //   throw new InternalError();
            if (sibling.getColor(wrapper) == Color.RED) {
               // Case 1: Sibling is red.
               // Recolor sibling and parent, and rotate parent left.
               sibling.setColor(wrapper, Color.BLACK);
               parent.setColor(wrapper, Color.RED);
               rotateLeft(wrapper, parent);
               sibling = parent.getRight(wrapper);
            }

            if (sibling.getLeft(wrapper).getColor(wrapper) == Color.BLACK && sibling.getRight(wrapper).getColor(wrapper) == Color.BLACK) {
               // Case 2: Sibling has no red children.
               // Recolor sibling, and move to parent.
               sibling.setColor(wrapper, Color.RED);
               node = parent;
               parent = parent.getParent(wrapper);
            } else {
               if (sibling.getRight(wrapper).getColor(wrapper) == Color.BLACK) {
                  // Case 3: Sibling has red left child.
                  // Recolor sibling and left child, rotate sibling right.
                  sibling.getLeft(wrapper).setColor(wrapper, Color.BLACK);
                  sibling.setColor(wrapper, Color.RED);
                  rotateRight(wrapper, sibling);
                  sibling = parent.getRight(wrapper);
               }
               // Case 4: Sibling has red right child. Recolor sibling,
               // right child, and parent, and rotate parent left.
               sibling.setColor(wrapper, parent.getColor(wrapper));
               parent.setColor(wrapper, Color.BLACK);
               sibling.getRight(wrapper).setColor(wrapper, Color.BLACK);
               rotateLeft(wrapper, parent);
               node = getRoot(wrapper); // Finished.
            }
         } else {
            // Symmetric "mirror" of left-side case.
            Node sibling = parent.getLeft(wrapper);
            // if (sibling == nil)
            //   throw new InternalError();
            if (sibling.getColor(wrapper) == Color.RED) {
               // Case 1: Sibling is red.
               // Recolor sibling and parent, and rotate parent right.
               sibling.setColor(wrapper, Color.BLACK);
               parent.setColor(wrapper, Color.RED);
               rotateRight(wrapper, parent);
               sibling = parent.getLeft(wrapper);
            }

            if (sibling.getRight(wrapper).getColor(wrapper) == Color.BLACK && sibling.getLeft(wrapper).getColor(wrapper) == Color.BLACK) {
               // Case 2: Sibling has no red children.
               // Recolor sibling, and move to parent.
               sibling.setColor(wrapper, Color.RED);
               node = parent;
               parent = parent.getParent(wrapper);
            } else {
               if (sibling.getLeft(wrapper).getColor(wrapper) == Color.BLACK) {
                  // Case 3: Sibling has red right child.
                  // Recolor sibling and right child, rotate sibling left.
                  sibling.getRight(wrapper).setColor(wrapper, Color.BLACK);
                  sibling.setColor(wrapper, Color.RED);
                  rotateLeft(wrapper, sibling);
                  sibling = parent.getLeft(wrapper);
               }
               // Case 4: Sibling has red left child. Recolor sibling,
               // left child, and parent, and rotate parent right.
               sibling.setColor(wrapper, parent.getColor(wrapper));
               parent.setColor(wrapper, Color.BLACK);
               sibling.getLeft(wrapper).setColor(wrapper, Color.BLACK);
               rotateRight(wrapper, parent);
               node = getRoot(wrapper); // Finished.
            }
         }
      }
      node.setColor(wrapper, Color.BLACK);
   }

   final Node firstNode(CacheWrapper wrapper) {
      // Exploit fact that nil.left == nil.
      Node node = getRoot(wrapper);
      while (!node.getLeft(wrapper).isNil())
         node = node.getLeft(wrapper);
      return node;
   }

   final Node getNode(CacheWrapper wrapper, K key) {
      Node current = getRoot(wrapper);
      while (!current.isNil()) {
         int comparison = key.compareTo(current.getKey(wrapper));
         if (comparison > 0)
            current = current.getRight(wrapper);
         else if (comparison < 0)
            current = current.getLeft(wrapper);
         else
            return current;
      }
      return current;
   }

   private void insertFixup(CacheWrapper wrapper, Node n) {
      // Only need to rebalance when parent is a RED node, and while at least
      // 2 levels deep into the tree (ie: node has a grandparent). Remember
      // that nil.color == BLACK.
      while (n.getParent(wrapper).getColor(wrapper) == Color.RED && !n.getParent(wrapper).getParent(wrapper).isNil()) {
         if (n.getParent(wrapper) == n.getParent(wrapper).getParent(wrapper).getLeft(wrapper)) {
            Node uncle = n.getParent(wrapper).getParent(wrapper).getRight(wrapper);
            // Uncle may be nil, in which case it is BLACK.
            if (uncle.getColor(wrapper) == Color.RED) {
               // Case 1. Uncle is RED: Change colors of parent, uncle,
               // and grandparent, and move n to grandparent.
               n.getParent(wrapper).setColor(wrapper, Color.BLACK);
               uncle.setColor(wrapper, Color.BLACK);
               uncle.getParent(wrapper).setColor(wrapper, Color.RED);
               n = uncle.getParent(wrapper);
            } else {
               if (n == n.getParent(wrapper).getRight(wrapper)) {
                  // Case 2. Uncle is BLACK and x is right child.
                  // Move n to parent, and rotate n left.
                  n = n.getParent(wrapper);
                  rotateLeft(wrapper, n);
               }
               // Case 3. Uncle is BLACK and x is left child.
               // Recolor parent, grandparent, and rotate grandparent right.
               n.getParent(wrapper).setColor(wrapper, Color.BLACK);
               n.getParent(wrapper).getParent(wrapper).setColor(wrapper, Color.RED);
               rotateRight(wrapper, n.getParent(wrapper).getParent(wrapper));
            }
         } else {
            // Mirror image of above code.
            Node uncle = n.getParent(wrapper).getParent(wrapper).getLeft(wrapper);
            // Uncle may be nil, in which case it is BLACK.
            if (uncle.getColor(wrapper) == Color.RED) {
               // Case 1. Uncle is RED: Change colors of parent, uncle,
               // and grandparent, and move n to grandparent.
               n.getParent(wrapper).setColor(wrapper, Color.BLACK);
               uncle.setColor(wrapper, Color.BLACK);
               uncle.getParent(wrapper).setColor(wrapper, Color.RED);
               n = uncle.getParent(wrapper);
            } else {
               if (n == n.getParent(wrapper).getLeft(wrapper)) {
                  // Case 2. Uncle is BLACK and x is left child.
                  // Move n to parent, and rotate n right.
                  n = n.getParent(wrapper);
                  rotateRight(wrapper, n);
               }
               // Case 3. Uncle is BLACK and x is right child.
               // Recolor parent, grandparent, and rotate grandparent left.
               n.getParent(wrapper).setColor(wrapper, Color.BLACK);
               n.getParent(wrapper).getParent(wrapper).setColor(wrapper, Color.RED);
               rotateLeft(wrapper, n.getParent(wrapper).getParent(wrapper));
            }
         }
      }
      getRoot(wrapper).setColor(wrapper, Color.BLACK);
   }

   final void removeNode(CacheWrapper wrapper, Node node) {
      Node splice;
      Node child;

      // Find splice, the node at the position to actually remove from the tree.
      if (node.getLeft(wrapper).isNil()) {
         // Node to be deleted has 0 or 1 children.
         splice = node;
         child = node.getRight(wrapper);
      } else if (node.getRight(wrapper).isNil()) {
         // Node to be deleted has 1 child.
         splice = node;
         child = node.getLeft(wrapper);
      } else {
         // Node has 2 children. Splice is node's predecessor, and we swap
         // its contents into node.
         splice = node.getLeft(wrapper);
         while (!splice.getRight(wrapper).isNil())
            splice = splice.getRight(wrapper);
         child = splice.getLeft(wrapper);
         node.setKey(wrapper, splice.getKey(wrapper));
         node.setValue(wrapper, splice.getValue(wrapper));
      }

      // Unlink splice from the tree.
      Node parent = splice.getParent(wrapper);
      if (!child.isNil())
         child.setParent(wrapper, parent);
      if (parent.isNil()) {
         // Special case for 0 or 1 node remaining.
         setRoot(wrapper, child);
         return;
      }
      if (splice == parent.getLeft(wrapper))
         parent.setLeft(wrapper, child);
      else
         parent.setRight(wrapper, child);

      if (splice.getColor(wrapper) == Color.BLACK)
         deleteFixup(wrapper, child, parent);
   }

   private void rotateLeft(CacheWrapper wrapper, Node node) {
      Node child = node.getRight(wrapper);
      // if (node == nil || child == nil)
      //   throw new InternalError();

      // Establish node.right link.
      node.setRight(wrapper, child.getLeft(wrapper));
      if (!child.getLeft(wrapper).isNil())
         child.getLeft(wrapper).setParent(wrapper, node);

      // Establish child->parent link.
      child.setParent(wrapper, node.getParent(wrapper));
      if (!node.getParent(wrapper).isNil()) {
         if (node == node.getParent(wrapper).getLeft(wrapper))
            node.getParent(wrapper).setLeft(wrapper, child);
         else
            node.getParent(wrapper).setRight(wrapper, child);
      } else
         setRoot(wrapper, child);

      // Link n and child.
      child.setLeft(wrapper, node);
      node.setParent(wrapper, child);
   }

   private void rotateRight(CacheWrapper wrapper, Node node) {
      Node child = node.getLeft(wrapper);
      // if (node == nil || child == nil)
      //   throw new InternalError();

      // Establish node.left link.
      node.setLeft(wrapper, child.getRight(wrapper));
      if (!child.getRight(wrapper).isNil())
         child.getRight(wrapper).setParent(wrapper, node);

      // Establish child->parent link.
      child.setParent(wrapper, node.getParent(wrapper));
      if (!node.getParent(wrapper).isNil()) {
         if (node == node.getParent(wrapper).getRight(wrapper))
            node.getParent(wrapper).setRight(wrapper, child);
         else
            node.getParent(wrapper).setLeft(wrapper, child);
      } else
         setRoot(wrapper, child);

      // Link n and child.
      child.setRight(wrapper, node);
      node.setParent(wrapper, child);
   }

   final Node successor(CacheWrapper wrapper, Node node) {
      if (!node.getRight(wrapper).isNil()) {
         node = node.getRight(wrapper);
         while (!node.getLeft(wrapper).isNil())
            node = node.getLeft(wrapper);
         return node;
      }

      Node parent = node.getParent(wrapper);
      // Exploit fact that nil.right == nil and node is non-nil.
      while (node == parent.getRight(wrapper)) {
         node = parent;
         parent = parent.getParent(wrapper);
      }
      return parent;
   }

   public final class Node implements Serializable {

      private String uuid;
      private boolean isNil;

      public Node(CacheWrapper wrapper, K key, V value, Color color) {
         this.uuid = UUID.randomUUID().toString();
         setKey(wrapper, key);
         setValue(wrapper, value);
         setColor(wrapper, color);
         setLeft(wrapper, myNilNode);
         setRight(wrapper, myNilNode);
         setParent(wrapper, myNilNode);
      }

      public Node(CacheWrapper wrapper, K key, V value, Color color, boolean isNil) {
         this.isNil = isNil;
         this.uuid = UUID.randomUUID().toString();
         setKey(wrapper, key);
         setValue(wrapper, value);
         setColor(wrapper, color);
      }

      public boolean isNil() {
         return this.isNil;
      }

      public Color getColor(CacheWrapper wrapper) {
         return (Color) Micro.get(wrapper, uuid + ":color");
      }

      public void setColor(CacheWrapper wrapper, Color newColor) {
         Micro.put(wrapper, uuid + ":color", newColor);
      }

      public Node getParent(CacheWrapper wrapper) {
         return (Node) Micro.get(wrapper, uuid + ":parent");
      }

      public void setParent(CacheWrapper wrapper, Node newParent) {
         Micro.put(wrapper, uuid + ":parent", newParent);
      }

      public Node getLeft(CacheWrapper wrapper) {
         return (Node) Micro.get(wrapper, uuid + ":left");
      }

      public void setLeft(CacheWrapper wrapper, Node newLeft) {
         Micro.put(wrapper, uuid + ":left", newLeft);
      }

      public Node getRight(CacheWrapper wrapper) {
         return (Node) Micro.get(wrapper, uuid + ":right");
      }

      public void setRight(CacheWrapper wrapper, Node newRight) {
         Micro.put(wrapper, uuid + ":right", newRight);
      }

      public V getValue(CacheWrapper wrapper) {
         return (V) Micro.get(wrapper, uuid + ":value");
      }

      public void setValue(CacheWrapper wrapper, V newValue) {
         Micro.put(wrapper, uuid + ":value", newValue);
      }

      public K getKey(CacheWrapper wrapper) {
         return (K) Micro.get(wrapper, uuid + ":key");
      }

      public void setKey(CacheWrapper wrapper, K newKey) {
         Micro.put(wrapper, uuid + ":key", newKey);
      }

   }

}
