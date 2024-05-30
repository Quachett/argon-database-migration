package uk.co.argon.db.migration.frontend;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import uk.co.argon.common.exceptions.HttpException;
import uk.co.argon.common.fileio.FileManager;
import uk.co.argon.common.util.JsonParser;
import uk.co.argon.db.migration.backend.Facade;
import uk.co.argon.db.migration.backend.FacadeBean;
import uk.co.argon.db.migration.backend.dao.GenerateQueriesDao;
import uk.co.argon.db.migration.backend.dao.GenerateQueriesDaoBean;
import uk.co.argon.db.migration.backend.dao.TableCreationAndPopulationDao;
import uk.co.argon.db.migration.backend.dao.TableCreationAndPopulationDaoBean;
import uk.co.argon.db.migration.backend.multithreading.PopulateTableFixedThreadPoolExecutor;
import uk.co.argon.db.migration.backend.multithreading.PopulateTableRunnable;
import uk.co.argon.db.migration.backend.util.DatatypesQueryConstructor;
import uk.co.argon.db.migration.backend.util.Util;
import uk.co.argon.db.migration.backend.vo.Query;

public class ServiceMain {
	
	private static final String PATH = "C:/Users/lloyd/OneDrive/dir/mfdb/";
	private static final String FILENAME = "ovaflo_tables.txt";
	private static ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, PopulateTableRunnable> runnableMap = new ConcurrentHashMap<>();
	
	private static GenerateQueriesDao dao = new GenerateQueriesDaoBean();
	private static final TableCreationAndPopulationDao popDao = new TableCreationAndPopulationDaoBean();
	
	//@Inject
	private static Facade facade = new FacadeBean(new TableCreationAndPopulationDaoBean());
	
	public static void main(String[] args) throws SQLException, HttpException, IOException, InterruptedException {
		//test();
		Long t = System.currentTimeMillis();
		generateQueries();
		createTables();
		createRunnables();
		populateTables();
		System.out.println("Time taken: " + ((double)(System.currentTimeMillis() - t))/1000.0);
		//System.out.println(twoYearsAgo());
	}
	
	private static void generateQueries() throws IOException, SQLException, HttpException {
		List<String> tables = FileManager.getInstance().fileToList(PATH, FILENAME);
		
		dao.generateQueries(tables, queries);
	}
	
	private static void createTables() throws SQLException, HttpException {
		facade.createTableCreationOrder(queries.keySet());
		facade.createTables(queries);
		facade.updateInsertionQueries(queries);
		facade.createTablePopulationOrder(queries.keySet());
		
		System.out.println("\n" + "=".repeat(15) + " Queries " + "=".repeat(15));
		System.out.println(JsonParser.getInstance().serialise(queries));
		System.out.println("=".repeat(39) + "\n");
	}
	
	private static void createRunnables() throws HttpException {
		queries.forEach((k, v) -> runnableMap.put(k, new PopulateTableRunnable(v, v.getStart(), v.getEnd(), popDao, k)));
		addDependencies();
	}
	
	private static void addDependencies() {
		ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> deps = DatatypesQueryConstructor.getInstance().getTableDependancies();
		runnableMap.forEach((k, v) -> {
			if(deps.get(k)!=null)
				deps.get(k).forEach(s -> v.addDependency(runnableMap.get(s)));
		});
	}
	
	private static void populateTables() throws InterruptedException {
		PopulateTableFixedThreadPoolExecutor exec =new PopulateTableFixedThreadPoolExecutor(Util.CORE_POOL_SIZE,
				Util.MAX_POOL_SIZE,
				Util.KEEP_ALIVE_TIME,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		
		System.out.println("\n" + "=".repeat(15) + " Populating Synapse tables " + "=".repeat(15));
		
		if(popDao.setForeignKeyCheckStatus(Util.DISABLE))
			runnableMap.forEach((k,v)-> exec.execute(v));
		else
			System.out.println("Failed to disable Foreign Key checks");
		
		exec.awaitTermination(120, TimeUnit.MINUTES);
		
		if(!popDao.setForeignKeyCheckStatus(Util.ENABLE)) {
			System.out.println("Failed to enable Foreign Key checks");
			exec.shutdown();
		}
		System.out.println("=".repeat(53) + "\n");
	}
	
	public static void test() {
		
		DatatypesQueryConstructor.getInstance().addTableDependancies("A", getSet(Arrays.asList("B", "D", "E", "H")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("B", getSet(Arrays.asList("D", "E")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("C", getSet(Arrays.asList("D", "E", "I", "K", "L")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("D", getSet(Arrays.asList()));
		DatatypesQueryConstructor.getInstance().addTableDependancies("E", getSet(Arrays.asList("M")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("F", getSet(Arrays.asList()));
		DatatypesQueryConstructor.getInstance().addTableDependancies("G", getSet(Arrays.asList("H", "I")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("H", getSet(Arrays.asList()));
		DatatypesQueryConstructor.getInstance().addTableDependancies("I", getSet(Arrays.asList("D", "N")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("J", getSet(Arrays.asList("D", "C")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("K", getSet(Arrays.asList("E", "L")));
		DatatypesQueryConstructor.getInstance().addTableDependancies("L", getSet(Arrays.asList()));
		DatatypesQueryConstructor.getInstance().addTableDependancies("M", getSet(Arrays.asList()));
		DatatypesQueryConstructor.getInstance().addTableDependancies("N", getSet(Arrays.asList()));
		
		for(String s: DatatypesQueryConstructor.getInstance().getTableDependancies().keySet()) {
			if(StringUtils.equalsAnyIgnoreCase(s, "A", "E", "L", "M"))
				queries.put(s,new Query("Drop", "Create", s, "Insert", 1, 5000000, null, null));
			else
				queries.put(s,new Query("Drop", "Create", s, "Insert", 1, 20000, null, null));
		}
	}
	
	private static ConcurrentHashMap.KeySetView<String, Boolean> getSet(List<String> al) {
		ConcurrentHashMap.KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet();
		set.addAll(al);
		return set;
	}
	
	private static String twoYearsAgo() {
		LocalDateTime twoYearsAgo = LocalDateTime.now().minusMonths(29);
		return twoYearsAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}
}
