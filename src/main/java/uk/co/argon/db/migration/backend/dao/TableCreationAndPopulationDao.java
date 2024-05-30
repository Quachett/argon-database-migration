package uk.co.argon.db.migration.backend.dao;

import java.sql.SQLException;
import java.util.Map;

import uk.co.argon.db.migration.backend.vo.Query;

public interface TableCreationAndPopulationDao {
	public void createTablesInSynapse(Map<String, Query> queries) throws SQLException;
	public void migrateData(Query query, int start, int end);
	public boolean setForeignKeyCheckStatus(int values);
}
