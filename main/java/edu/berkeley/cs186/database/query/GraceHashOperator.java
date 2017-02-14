package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    TableStats leftStats = getLeftSource().getStats();
    TableStats rightStats = getRightSource().getStats();

    return 3 * (leftStats.getNumPages() + rightStats.getNumPages());
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private Record rightRecord;
    private Record nextRecord;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private int currentPartition;
    private Map<DataType, ArrayList<Record>> inMemoryHashTable;

    private Iterator<Record> rightTableIterator;
    private Iterator<Record> leftRecordIterator;

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }

      // TODO: implement me!
      while (leftIterator.hasNext()) {
        Record r = leftIterator.next();
        DataType joinCol = r.getValues().get(getLeftColumnIndex());
        int hashIndex = joinCol.hashCode() % leftPartitions.length;
        GraceHashOperator.this.addRecord(leftPartitions[hashIndex], r.getValues());
      }

      while (rightIterator.hasNext()) {
        Record r = rightIterator.next();
        DataType joinCol = r.getValues().get(getRightColumnIndex());
        int hashIndex = joinCol.hashCode() % rightPartitions.length;
        GraceHashOperator.this.addRecord(rightPartitions[hashIndex], r.getValues());
      }

      fillHashTable();
      rightTableIterator = getTableIterator(rightPartitions[currentPartition]);
      leftRecordIterator = Collections.emptyIterator();
    }

    private void fillHashTable() throws DatabaseException {
      inMemoryHashTable = new HashMap<DataType, ArrayList<Record>>();
      Iterator<Record> leftPartitionIterator = getTableIterator(leftPartitions[currentPartition]);
      while (leftPartitionIterator.hasNext()) {
        Record r = leftPartitionIterator.next();
        DataType orderValue = r.getValues().get(getLeftColumnIndex());
        ArrayList<Record> records;
        if (inMemoryHashTable.containsKey(orderValue)) {
          records = inMemoryHashTable.get(orderValue);
        } else {
          records = new ArrayList<Record>();
        }
        records.add(r);
        inMemoryHashTable.put(orderValue, records);
      }
    }
    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!
      if (nextRecord != null)
        return true;

      if (leftRecordIterator.hasNext()) {
        List<DataType> leftValues = new ArrayList<DataType>(leftRecordIterator.next().getValues());
        List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        return true;
      }

      while (true) {
        while (rightTableIterator.hasNext()) {
          rightRecord = rightTableIterator.next();
          DataType orderValue = rightRecord.getValues().get(getRightColumnIndex());

          if (inMemoryHashTable.containsKey(orderValue)) {
            leftRecordIterator = inMemoryHashTable.get(orderValue).iterator();
            List<DataType> leftValues = new ArrayList<DataType>(leftRecordIterator.next().getValues());
            List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;
          }
        }

        if (currentPartition < leftPartitions.length-1) {
          try {
            currentPartition++;
            fillHashTable();
            rightTableIterator = getTableIterator(rightPartitions[currentPartition]);
          } catch (DatabaseException e) {
            return false;
          }
        } else {
          return false;
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
  }
}
