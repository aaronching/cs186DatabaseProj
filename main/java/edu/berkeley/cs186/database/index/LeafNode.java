package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    //TODO: Implement Me!!
    DataType min;
    try {
      min = getFirstValidEntry().getKey();
    } catch (NullPointerException e) {
      return this;
    }

    LeafNode prevLeaf;
    try {
      if (key.compareTo(min) == 0) {
        if (findFirst) {
          prevLeaf = (LeafNode) getBPlusNode(getTree(), getPrevLeaf());
          if (prevLeaf.getLastValidEntry().getKey().compareTo(key) == 0)
            return prevLeaf.locateLeaf(key, findFirst);
        }
      }
      return this;
    } catch (PageException e) {
      return this;
    }
  }

  /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
    //TODO: Implement Me!!
    LeafNode newNode = new LeafNode(getTree());
    newNode.setPrevLeaf(this.getPageNum());
    newNode.setNextLeaf(this.getNextLeaf());
    if (getNextLeaf() != -1) //set next leaf's previous pointer
      ((LeafNode)getBPlusNode(getTree(), getNextLeaf())).setPrevLeaf(newNode.getPageNum());
    this.setNextLeaf(newNode.getPageNum());

    List<BEntry> entries = getAllValidEntries();
    List<BEntry> oldNodeEntries = entries.subList(0, entries.size()/2); //first d entries
    List<BEntry> newNodeEntries = entries.subList(entries.size()/2, entries.size()); //the rest of the entries
    this.overwriteBNodeEntries(oldNodeEntries);
    newNode.overwriteBNodeEntries(newNodeEntries);

    if (this.getParent() == -1) {
      InnerNode newParent = new InnerNode(getTree());
      newParent.setFirstChild(this.getPageNum());
      this.setParent(newParent.getPageNum());
      getTree().updateRoot(getParent());
    }
    newNode.setParent(this.getParent());

    DataType splitKey = newNodeEntries.get(0).getKey();
    InnerEntry newEntry = new InnerEntry(splitKey, newNode.getPageNum());
    getBPlusNode(getTree(), this.getParent()).insertBEntry(newEntry);
  }
  
  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }

    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }
}
