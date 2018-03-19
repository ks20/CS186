package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

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

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * PNLJ: Page Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    //private Iterator<Page> leftIterator = null;
    //private Iterator<Page> rightIterator = null;

    private Iterator<Page> leftPageIter;
    private Iterator<Page> rightPageIter;
    private BacktrackingIterator<Record> leftRecordIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Page leftPage;
    private Page rightPage;
    private Page headerLeftPage;
    private Page headerRightPage;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private boolean flag;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftPageIter = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightPageIter = PNLJOperator.this.getPageIterator(this.getRightTableName());

      this.headerLeftPage = leftPageIter.next();
      this.headerRightPage = rightPageIter.next();

      this.leftPage = leftPageIter.next();
      this.rightPage = rightPageIter.next();

      this.leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), new Page[]{this.leftPage});
      this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{this.rightPage});

      this.leftRecord = null;
      this.rightRecord = null;
      this.nextRecord = null;
      this.flag = false;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }

      try {
        while (true) {
          if (this.leftRecord == null) {
            if (nextLeftRecord()) {
              this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{this.rightPage});
            }
            else if (!nextLeftRecord()){
              if (this.rightPageIter.hasNext()) {
                this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), new Page[]{this.leftPage});
                nextLeftRecord();
              }
              else if (!this.rightPageIter.hasNext()){
                if (!this.leftPageIter.hasNext()) {
                  return false;
                }
                this.leftPage = this.leftPageIter.next();
                this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), new Page[]{this.leftPage});
                nextLeftRecord();
                this.rightPageIter = PNLJOperator.this.getPageIterator(this.getRightTableName());
                this.headerRightPage = this.rightPageIter.next();
              }

              this.rightPage = this.rightPageIter.next();
              this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), new Page[]{this.rightPage});
            }
          }

          while (this.rightRecordIterator.hasNext()) {
            fetchNextRecord();
            if (flag) {
              flag = false;
              return true;
            }
          }
          this.leftRecord = null;
        }
      } catch (DatabaseException exception) {
        return false;
      }
    }

    private void fetchNextRecord() throws DatabaseException {
      this.rightRecord = this.rightRecordIterator.next();
      DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
      DataBox rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
      if (leftJoinValue.equals(rightJoinValue)) {
        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        flag = true;
      }
    }

    private boolean nextLeftRecord() {
      if (!this.leftRecordIterator.hasNext()) {
        return false;
      }
      this.leftRecord = this.leftRecordIterator.next();
      return true;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.nextRecord == null) {
        throw new NoSuchElementException();
      }

      Record nextRec = this.nextRecord;
      this.nextRecord = null;
      return nextRec;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
