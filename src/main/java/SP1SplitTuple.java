package edu.brown.benchmark.tpcdi.orig.procedures;

import edu.brown.benchmark.tpcdi.orig.TPCDIConstants;
import edu.brown.benchmark.tpcdi.orig.TPCDIUtil;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
	partitionInfo = "SP1out.T_ID:2" //T_ID:2 table id?
)
public class SP1SplitTuple extends VoltProcedure { //VoltProcedure?
	
    public final SQLStmt SP1OutStmt = new SQLStmt(
	   "INSERT INTO SP1out VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    );
    
    //for Postgres
    /*
    //PREPARE SP1OutStmt [()] AS statement ?
    public final SQLStmt SP1OutStmt = new SQLStmt(
	   "EXEC SQL INSERT INTO SP1out (number, ascii) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
	   "EXEC SQL COMMIT;"
    );
    */

//if no value found replace with zero
    private String replaceWithZeroIfEmpty(String str) {
      if (str.isEmpty()) {
        return "0";
      } else {
        return str;
      }
    }
    public long run(long batchid, String[] tuples, long part_id) {
    	long tmpTime = System.currentTimeMillis();


      Long T_ID = null;

    	for(int i = 0; i < tuples.length; i++) {
    		String[] st = tuples[i].split("\\|", -1);
        T_ID = new Long(st[0]);
    		voltQueueSQL(SP1OutStmt, //st[0],new Long(st[1]),
    								T_ID,st[1],st[2]
    								 ,st[3],new Short(st[4]),st[5],new Integer(st[6])
    								 ,new Double(st[7]),new Integer(st[8]),st[9],new Double(replaceWithZeroIfEmpty(st[10])),
    								 new Double(replaceWithZeroIfEmpty(st[11])),new Double(replaceWithZeroIfEmpty(st[12])),new Double(replaceWithZeroIfEmpty(st[13])),
    								 batchid,
                     part_id
    								);
    								
    		//voltQueueSQL?

      }
    	//voltExecuteSQL();
        int destinationPartition = TPCDIUtil.hashCode(String.valueOf(T_ID), TPCDIConstants.NUM_PARTITIONS);
        voltExecuteSQLDownStream("SP1out", destinationPartition);
        
        //voltExecuteSQLDownStream?
        //voltExecuteSQL()?


        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}
