package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    TableStats leftStats = getLeftSource().getStats();
    TableStats rightStats = getRightSource().getStats();

    return (int)Math.ceil((double)leftStats.getNumPages()/(numBuffers-2)) * rightStats.getNumPages() + leftStats.getNumPages();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private Page[] block;

    private Iterator<Record> leftBlockRecordsIterator;
    private Iterator<Record> rightPageRecordsIterator;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }

      // TODO: implement me!
      leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
      leftPage = leftIterator.next(); //skip header page
      setNextLeftBlock();
      if (leftBlockRecordsIterator().hasNext()) {
        leftRecord = leftBlockRecordsIterator.next();
      }
      setRight();

    }

    private void setNextLeftBlock() throws DatabaseException{
      block = new Page[numBuffers-2];
      for (int i=0; i<numBuffers-2; i++) {
        if (leftIterator.hasNext()) {
          leftPage = leftIterator.next();
          block[i] = leftPage;
        } else {
          break;
        }
      }
      leftBlockRecordsIterator = leftBlockRecordsIterator();
    }

    private void setRight() throws DatabaseException {
      rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
      rightPage = rightIterator.next(); //skip header page
      if (!rightIterator.hasNext()) {
        rightPageRecordsIterator = Collections.emptyIterator();
      } else {
        rightPage = rightIterator.next();
        rightPageRecordsIterator = rightPageRecordsIterator();
      }
    }
    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!
      if (nextRecord != null) {
        return true;
      }

      if (leftRecord == null) {
        return false;
      }

      while (true) {
        while (rightPageRecordsIterator.hasNext()) {
          rightRecord = rightPageRecordsIterator.next();

          DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
            List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;
          }
        }

        if (leftBlockRecordsIterator.hasNext()) {
          try {
            leftRecord = leftBlockRecordsIterator.next();
            rightPageRecordsIterator = rightPageRecordsIterator();
          } catch (DatabaseException e) {
            return false;
          }
        } else { //we've finished a simple nested loop join for block and this right page
          try {
            if (rightIterator.hasNext()) {
              rightPage = rightIterator.next();
              rightPageRecordsIterator = rightPageRecordsIterator();
              leftBlockRecordsIterator = leftBlockRecordsIterator();
              leftRecord = leftBlockRecordsIterator.next();
            } else { //we've finished joining a block with every page in the right table
              setNextLeftBlock();
              if (leftBlockRecordsIterator.hasNext()) {
                leftRecord = leftBlockRecordsIterator.next();
                setRight(); //reset right iterator
              } else {
                return false;
              }
            }
          } catch (DatabaseException e) {
            return false;
          }
        }
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      // TODO: implement me!
      if (this.hasNext()) {
        Record next = nextRecord;
        nextRecord = null;
        return next;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    private Iterator<Record> leftBlockRecordsIterator() throws DatabaseException {
      ArrayList<Record> records = new ArrayList<Record>();
      int recordSize = BNLJOperator.this.getLeftSource().getOutputSchema().getEntrySize();
      int headerSize = BNLJOperator.this.getHeaderSize(leftTableName);
      for (Page p : block) {
        if (p == null)
          break;
        int byteCounter = 0;
        for (byte b : getPageHeader(leftTableName, p)) {
          for (int i = 0; i < 8; i++) {
            if ((b & (1 << (7 - i))) != 0) {
              records.add(BNLJOperator.this.getLeftSource().getOutputSchema().decode(p.readBytes(headerSize + (byteCounter * 8 + i) * recordSize, recordSize)));
            }
          }
          byteCounter++;
        }
      }
      return records.iterator();
    }

    private Iterator<Record> rightPageRecordsIterator() throws DatabaseException {
      ArrayList<Record> records = new ArrayList<Record>();
      int recordSize = BNLJOperator.this.getRightSource().getOutputSchema().getEntrySize();
      int headerSize = BNLJOperator.this.getHeaderSize(rightTableName);
      int byteCounter = 0;
      for (byte b : getPageHeader(rightTableName, rightPage)) {
        for (int i = 0; i < 8; i++) {
          if ((b & (1 << (7-i))) != 0) {
            records.add(BNLJOperator.this.getRightSource().getOutputSchema().decode(rightPage.readBytes(headerSize+(byteCounter*8 + i)*recordSize, recordSize)));
          }
        }
        byteCounter++;
      }
      return records.iterator();
    }
  }
}
