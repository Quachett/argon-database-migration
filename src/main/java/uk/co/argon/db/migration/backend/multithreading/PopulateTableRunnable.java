package uk.co.argon.db.migration.backend.multithreading;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;

import uk.co.argon.db.migration.backend.PopulateTablesFacade;
import uk.co.argon.db.migration.backend.dao.TableCreationAndPopulationDao;
import uk.co.argon.db.migration.backend.vo.Query;

public class PopulateTableRunnable implements Runnable {
	private final int start, end;
	private final Query query;
	private final String tableName;
    private final AtomicBoolean hasRun;
	private final ConcurrentHashMap.KeySetView<PopulateTableRunnable, Boolean> dependancies;
	private final TableCreationAndPopulationDao dao;

	public PopulateTableRunnable(Query query, int start, int end, TableCreationAndPopulationDao dao, String tableName) {
		this.dao = dao;
		this.end = end;
		this.start = start;
		this.query = query;
		this.tableName = tableName;
		this.hasRun = new AtomicBoolean(false);
		this.dependancies = ConcurrentHashMap.newKeySet();
	}

	@Override
	public void run() {
		Thread.currentThread().setName(getThreadName());
		PopulateTablesFacade ptf = (query, start, end) -> {
			long t = System.currentTimeMillis();
			dao.migrateData(query, start, end);
			
			System.out.println(Thread.currentThread().getName() +
					" || Query: " +
					query.getSelectQuery() +
					" || time taken: " +
					((double)(System.currentTimeMillis() - t)/1000.0) +
					" secs.");
		};
		ptf.populateTable(query, start, end);
	}
	
	public void addDependency(PopulateTableRunnable ptt) {
		dependancies.add(ptt);
	}
	
	public ConcurrentHashMap.KeySetView<PopulateTableRunnable, Boolean> getDependencies() {
		return this.dependancies;
	}

    public boolean hasRun() {
        return hasRun.get();
    }

    public void markAsRun() {
        hasRun.set(true);
    }
    
    public String getQuery() {
    	return query.getSelectQuery();
    }
    
    private String getThreadName() {
    	String name = StringUtils.substringBefore(Thread.currentThread().getName(), "_");
    	return name + "_" +StringUtils.substringBefore(tableName, StringUtils.SPACE);
    }
}
