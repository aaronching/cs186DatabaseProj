package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class PNLJOperator extends JoinOperator {

    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.PNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        // TODO: implement me!
        TableStats leftStats = getLeftSource().getStats();
        TableStats rightStats = getRightSource().getStats();

        return leftStats.getNumPages() * rightStats.getNumPages() + leftStats.getNumPages();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class PNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private String rightTableName;
        private Iterator<Page> leftIterator;
        private Iterator<Page> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private Page leftPage;
        private Page rightPage;

        private Iterator<Record> leftPageRecordsIterator;
        private Iterator<Record> rightPageRecordsIterator;


        public PNLJIterator() throws QueryPlanException, DatabaseException {
            if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }

            if (PNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            // TODO: implement me!
            leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
            leftPage = leftIterator.next(); //skip header page
            if (!leftIterator.hasNext()) {
                leftPageRecordsIterator = Collections.emptyIterator();
            } else {
                leftPage = leftIterator.next();
                leftPageRecordsIterator = pageRecordsIterator(leftTableName, leftPage);
                leftRecord = leftPageRecordsIterator.next();
            }
            setRight();
        }

        private void setRight() throws DatabaseException {
            rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
            rightPage = rightIterator.next(); //skip header page
            if (!rightIterator.hasNext()) {
                rightPageRecordsIterator = Collections.emptyIterator();
            } else {
                rightPage = rightIterator.next();
                rightPageRecordsIterator = pageRecordsIterator(rightTableName, rightPage);
            }
        }

        public boolean hasNext() {
            // TODO: implement me!
            if (nextRecord != null) {
                return true;
            }

            while (true) {
                while (rightPageRecordsIterator.hasNext()) {
                    rightRecord = rightPageRecordsIterator.next();

                    DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());

                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    }
                }

                if (leftPageRecordsIterator.hasNext()) {
                    try {
                        leftRecord = leftPageRecordsIterator.next();
                        rightPageRecordsIterator = pageRecordsIterator(rightTableName, rightPage);
                    } catch (DatabaseException e) {
                        return false;
                    }
                } else { //we've finished a simple nested loop join for these two pages
                    try {
                        if (rightIterator.hasNext()) {
                            rightPage = rightIterator.next();
                            rightPageRecordsIterator = pageRecordsIterator(rightTableName, rightPage);
                            leftPageRecordsIterator = pageRecordsIterator(leftTableName, leftPage);
                            leftRecord = leftPageRecordsIterator.next();
                        } else { //we've finished joining a left page with every page in the right table
                            if (leftIterator.hasNext()) {
                                leftPage = leftIterator.next();
                                leftPageRecordsIterator = pageRecordsIterator(leftTableName, leftPage);
                                leftRecord = leftPageRecordsIterator.next();
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

        private Iterator<Record> pageRecordsIterator(String table, Page p) throws DatabaseException {
            ArrayList<Record> records = new ArrayList<Record>();
            QueryOperator source = null;
            if (table.equals(leftTableName)) source = PNLJOperator.this.getLeftSource();
            else if (table.equals(rightTableName)) source = PNLJOperator.this.getRightSource();
            else throw new DatabaseException("Wrong table name");
            int recordSize = source.getOutputSchema().getEntrySize();
            int headerSize = PNLJOperator.this.getHeaderSize(table);
            int byteCounter = 0;
            for (byte b : getPageHeader(table, p)) {
                for (int i = 0; i < 8; i++) {
                    if ((b & (1 << (7-i))) != 0) {
                        records.add(source.getOutputSchema().decode(p.readBytes(headerSize+(byteCounter*8 + i)*recordSize, recordSize)));
                    }
                }
                byteCounter++;
            }
            return records.iterator();
        }
    }
}
