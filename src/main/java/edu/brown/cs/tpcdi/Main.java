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
				.desc("bulk load historic data, with optional boolean <run benchmark> to run benchmark after loading (defaults to false)")
				.required(false)
				.longOpt("load")
				.hasArg(false)
				.optionalArg(true)
				.argName("run benchmark")
				.type(Boolean.class)
				.build();
		opt.addOption(loader);
		
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
		boolean runBenchmark = true;
		if (line.hasOption("l")) {
			// determine if runBench argument is set
			Boolean runOpt = null;
			try {
				runOpt = (Boolean) line.getParsedOptionValue("l");
				if (runOpt == null) {
					runOpt = false;
				}
			} catch (ParseException e) {
				System.err.println( "-l arg parsing failed.  Reason: " + e.getMessage() );
				System.exit(1);
			}
			
			loadData = true;
			runBenchmark = runOpt.booleanValue();
		}
		
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
		System.out.println("Started TPCDI");
		
		String url = "jdbc:postgresql://127.0.0.1:5432/Lexi";
		String user = "Lexi";
		String password = "";
		Connection dbConn = DriverManager.getConnection(url, user, password);
		
		System.out.println("Connected to DB");

		// load data
		if (loadData) {
			TPCDILoader loader = new TPCDILoader(dbConn);
			System.out.println("Creating Tables");
			loader.createTables(dbConn);
			System.out.println("Loading...");
			loader.load();
			System.out.println("Finished Loading");
		}
		
		// stop if not running benchmark (only when bulk loading)
		if (!runBenchmark) {
			return;
		}
		
		// put all benchmark running code below!!
	}

}
