package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;

import java.util.List;

/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    //TODO: Implement Me!!
    List<BEntry> entries = getAllValidEntries();
    BPlusNode child;
    if (key.compareTo(entries.get(0).getKey()) < 0) {
      child = getBPlusNode(getTree(), getFirstChild());
      return child.locateLeaf(key, findFirst);
    }
    for (int i=0; i<entries.size()-1; i++) {
      BEntry smallerEntry = entries.get(i);
      BEntry largerEntry = entries.get(i+1);
      if (key.compareTo(smallerEntry.getKey()) >= 0 && key.compareTo(largerEntry.getKey()) < 0) {
        child = getBPlusNode(getTree(), smallerEntry.getPageNum());
        return child.locateLeaf(key, findFirst);
      }
    }
    if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
      child = getBPlusNode(getTree(), entries.get(entries.size()-1).getPageNum());
      return child.locateLeaf(key, findFirst);
    }

    return null; //shouldn't reach this case
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
    //TODO: Implement me!!
    InnerNode newNode = new InnerNode(getTree());
    List<BEntry> entries = getAllValidEntries();
    List<BEntry> oldNodeEntries = entries.subList(0, entries.size()/2);
    List<BEntry> newNodeEntries = entries.subList(entries.size()/2 + 1, entries.size());

    this.overwriteBNodeEntries(oldNodeEntries);

    if (this.getParent() == -1) {
      InnerNode newParent = new InnerNode(getTree());
      newParent.setFirstChild(this.getPageNum());
      this.setParent(newParent.getPageNum());
      getTree().updateRoot(getParent());
    }
    newNode.setParent(this.getParent());

    //set newNode to be parent of the right d-1 entries
    for (BEntry entry : newNodeEntries) {
      BPlusNode childNode = getBPlusNode(getTree(), entry.getPageNum());
      childNode.setParent(newNode.getPageNum());
    }

    newNode.overwriteBNodeEntries(newNodeEntries);
    newNode.setFirstChild(entries.get(entries.size()/2).getPageNum());

    //entry in parent now has to refer to new created node, so we create new InnerEntry with same key
    InnerEntry newEntry = new InnerEntry(entries.get(entries.size()/2).getKey(), newNode.getPageNum());
    getBPlusNode(getTree(), this.getParent()).insertBEntry(newEntry);

    return;
  }
}
