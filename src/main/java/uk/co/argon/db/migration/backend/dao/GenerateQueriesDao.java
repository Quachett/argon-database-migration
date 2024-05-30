package uk.co.argon.db.migration.backend.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import uk.co.argon.common.exceptions.HttpException;
import uk.co.argon.db.migration.backend.vo.Query;

public interface GenerateQueriesDao {

	public void generateQueries(List<String> tables, Map<String, Query> queries) throws SQLException, HttpException;
}
