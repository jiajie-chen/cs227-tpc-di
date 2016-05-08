package edu.brown.cs.tpcdi;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class Main {

	public static void main(String[] args) {
		// build the options list
		Options opt = new Options();
		// add the option for loading
		Option loader = Option.builder("l")
				.required(false)
				.longOpt("load")
				.build();
		Option benchmark = Option.builder("b")
				.required(false)
				.longOpt("benchmark")
				.build();
		opt.addOption(loader);
		opt.addOption(benchmark);
		
		// parse all options from command line
		CommandLineParser p = new DefaultParser();
		CommandLine line = null;
		try  {
			line = p.parse(opt, args);
		} catch(ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			System.exit(1);
		}
		
		// accumulate command line options and run
		
		boolean loadData = false;
		boolean runBenchmark = false;
		if (line.hasOption("l"))
			loadData = true;
		if (line.hasOption("b"))
			runBenchmark = true;
		// run TPCDI benchmark with the command line arguments
		try {
			runTPCDI(loadData, runBenchmark);
			//runTPCDI(true, false); // UNCOMMENT TO RUN JUST THE LOADER, COMMENT THE LINE ABOVE
		} catch (Exception e) {
			System.err.println( "Benchmark failed.  Reason: " + e.getMessage() );
			System.exit(1);
		}
	}

	private static void runTPCDI(boolean loadData, boolean runBenchmark) throws Exception {
		String url = "jdbc:postgresql://127.0.0.1:5432/" + TPCDIConstants.databaseName;
		String user = TPCDIConstants.databaseUsername;
		String password = TPCDIConstants.databasePassword;
		Connection dbConn = DriverManager.getConnection(url, user, password);
		
		System.out.println("Connected to PostgresDB");

		// load data
		if (loadData) {
			System.out.println("Creating Tables");
			TPCDILoader loader = new TPCDILoader(dbConn);
			System.out.println("Loading...");
			loader.load();
			System.out.println("Finished Loading");
		}
		
		// stop if not running benchmark (only when bulk loading)
		if (runBenchmark) {
			System.out.println("Started TPCDI");

			TPCDIClient client = new TPCDIClient(dbConn);
			client.createTable();
			client.run();
		}
		
		// put all benchmark running code below!!
	}

}
