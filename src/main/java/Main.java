import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
				System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
				System.exit(1);
			}
			
			loadData = true;
			runBenchmark = runOpt.booleanValue();
		}
		
		// run TPCDI benchmark with the command line arguments
		try {
			runTPCDI(loadData, runBenchmark);
		} catch (Exception e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			System.exit(1);
		}
	}

	private static void runTPCDI(boolean loadData, boolean runBenchmark) throws Exception {
		// setup DB connections
		String url = "jdbc:postgresql://localhost:5432/Lexi";
		Properties props = new Properties();
		props.setProperty("user","Lexi");
		props.setProperty("password","");
		Connection dbConn = DriverManager.getConnection(url, props);

		// load data
		if (loadData) {
			TPCDILoader loader = new TPCDILoader(dbConn);
			loader.createTables(dbConn);
			loader.load();
		}
		
		// stop if not running benchmark (only when bulk loading)
		if (!runBenchmark) {
			return;
		}
		
		// put all benchmark running code below!!
	}

}
