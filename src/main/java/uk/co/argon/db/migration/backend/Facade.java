package uk.co.argon.db.migration.backend;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import uk.co.argon.common.exceptions.HttpException;
import uk.co.argon.db.migration.backend.vo.Query;

public interface Facade {
	public void createTableCreationOrder(Set<String> tables) throws HttpException;
	public void createTablePopulationOrder(Set<String> tables);
	public void createTables(ConcurrentHashMap<String, Query> queries) throws SQLException;
	public void updateInsertionQueries(ConcurrentHashMap<String, Query> queries) throws HttpException;
}
