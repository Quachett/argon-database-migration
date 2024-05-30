package uk.co.argon.db.migration.backend.dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;

import uk.co.argon.common.exceptions.HttpException;
import uk.co.argon.db.migration.backend.util.DatatypesQueryConstructor;
import uk.co.argon.db.migration.backend.util.Util;
import uk.co.argon.db.migration.backend.vo.Column;
import uk.co.argon.db.migration.backend.vo.Constraint;
import uk.co.argon.db.migration.backend.vo.Query;

public class GenerateQueriesDaoBean implements GenerateQueriesDao {
	private static final String TABLE = "[table]";
	private static final String COLUMNS = "[columns]";
	private static final String VALUES = "[values]";
	
	private static final String OVAFLO_USERNAME = "argon";
	private static final String OVAFLO_PASSWORD = "Q15_4r90n";
	//private static final String OVAFLO_DB_Url = "jdbc:sqlserver://;serverName=localhost;port=1433;databaseName=MFDB;trustServerCertificate=true";
	private static final String OVAFLO_DB_Url = "jdbc:sqlserver://localhost:1433;databaseName=MFDB;trustServerCertificate=true";
	
	@Override
	public void generateQueries(List<String> tables, Map<String, Query> queries) throws SQLException, HttpException {
		try(Connection conn = DriverManager.getConnection(OVAFLO_DB_Url, OVAFLO_USERNAME, OVAFLO_PASSWORD)) {
			DatabaseMetaData dbmd = conn.getMetaData();
			for(String t: tables) {
				try(ResultSet rs = dbmd.getColumns(null, null, StringUtils.substringBefore(t, StringUtils.SPACE), null)) {
					generateSelectAndInsertQueries(rs, t, queries, conn);
				}
			}
			generateConstraintQueries(queries, conn);
			createTableQueries(queries);
		}
	}

	private void generateSelectAndInsertQueries(ResultSet rs1, String tableName, Map<String, Query> queries, Connection conn) throws SQLException, HttpException {
		int count = 0;
		Query q =  new Query();
		StringBuilder sb = new StringBuilder();
		
		while(rs1.next()) {
			++count;
			String name = Util.toCamelCase(rs1.getString("COLUMN_NAME"));
			int columnSize = Integer.parseInt(rs1.getString("COLUMN_SIZE"));
			String dataType = rs1.getString("TYPE_NAME").replace(" identity", "");
			boolean isNullable = StringUtils.equalsIgnoreCase(rs1.getString("IS_NULLABLE"), "YES");
			
			dataType = (StringUtils.equalsAnyIgnoreCase(dataType, "varchar", "uniqueidentifier"))? Util.getTextField(columnSize): dataType;
			
			q.getColumns().add(new Column(name, dataType, columnSize, isNullable));
			
			sb.append("`" + name + "`" + ", ");
		}
		
		String questionMarks = Util.getQuestionMarks(count);
		String columns = Util.clean(sb.toString());
		
		q.setSelectQuery(StringUtils.replace(Util.SELECT_STATEMENT, TABLE, tableName));
		tableName = StringUtils.substringBefore(tableName, StringUtils.SPACE);
		q.setInsertQuery(Util.INSERT_STATEMENT.replace(TABLE, tableName).replace(COLUMNS, columns).replace(VALUES, questionMarks));
		
		int[] arr = getRecordsCount(q.getSelectQuery(), conn);
		q.setStart(arr[0]);
		q.setEnd(arr[1]);
		
		queries.put(tableName, q);
	}

	private void generateConstraintQueries(Map<String, Query> queries, Connection conn) throws SQLException, HttpException {
	    SQLServerDatabaseMetaData dbmd = (SQLServerDatabaseMetaData) conn.getMetaData();
	    
	    for(Entry<String, Query> eq: queries.entrySet()) {
	    	Query q = eq.getValue();
		    Constraint constraint = new Constraint();
	        try(ResultSet foreignKeys = dbmd.getImportedKeys(null, null, eq.getKey());
	        		ResultSet primaryKey = dbmd.getPrimaryKeys(null, null, eq.getKey())) {

        		getPKConstraintQuery(primaryKey, constraint, eq.getKey());	        	
	            
	        	getFKConstraintQueries(foreignKeys, constraint, eq.getKey());
	        	
	        	q.setConstraints(constraint);
	        }
	    }
	}
	
	private void getPKConstraintQuery(ResultSet primaryKey, Constraint constraint, String table) throws SQLException {
		if(primaryKey.next()) {
			String pk_constraint =  Util.PK_CONSTRAINT.replace(TABLE, table)
				.replace("[pkcolumn_name]", Util.toCamelCase(primaryKey.getString("COLUMN_NAME")));
			
			constraint.setPrimaryKey(pk_constraint);
		}
	}
	
	private void getFKConstraintQueries(ResultSet foreignKeys, Constraint constraint, String table) throws SQLException {
		ConcurrentHashMap.KeySetView<String, Boolean> tableUncestors = ConcurrentHashMap.newKeySet();
		while (foreignKeys.next()) {
            String fk_name = foreignKeys.getString("FK_NAME");
            String pktable_name = foreignKeys.getString("PKTABLE_NAME").toUpperCase();
            String fkcolumn_name = Util.toCamelCase(foreignKeys.getString("FKCOLUMN_NAME"));
            String pkcolumn_name = Util.toCamelCase(foreignKeys.getString("PKCOLUMN_NAME"));
            
            String fk_constraint = Util.FK_CONSTRAINT.replace(TABLE, table)
            		.replace("[fk_name]", fk_name).replace("[fkcolumn_name]", fkcolumn_name)
            		.replace("[pktable_name]", pktable_name).replace("[pkcolumn_name]", pkcolumn_name);
            
            tableUncestors.add(pktable_name);
            
            constraint.getForeignKey().add(fk_constraint);
        }
		DatatypesQueryConstructor.getInstance().addTableDependancies(table, tableUncestors);
	}

	
	private int[] getRecordsCount(String query, Connection conn) throws SQLException {
		int[] result = new int[2];
		
		query = query.replace("*", "id");
		
		try(PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
			if(StringUtils.containsIgnoreCase(query, "t.dateAdded"))
				ps.setString(1, Util.twoYearsAgo());
			
			try(ResultSet rs = ps.executeQuery()){
				if(rs.last()) {
					result[1] = rs.getInt("id");
					rs.beforeFirst();
				}
				if(rs.next()) 
					result[0] = rs.getInt("id");
			}

			if(StringUtils.containsIgnoreCase(query, "LOB_ASS_ASSESSMENTITEM"))
				System.out.println("Start: " + result[0] +
					"\tEnd: " + result[1] +
					"\tnumRec: " + (result[1] - result[0]) +
					"\tTimestamp: " + Util.twoYearsAgo());
		}
		return result;
	}
	
	private void createTableQueries(Map<String, Query> queries) throws HttpException {
		StringBuilder sb = null;
		for(Entry<String, Query> eq: queries.entrySet()) {
	    	Query q = eq.getValue();
			sb = new StringBuilder();
			for(Column c: q.getColumns()) {
				sb.append("`" + c.getName() + "`").append(StringUtils.SPACE)
					.append(DatatypesQueryConstructor.getInstance()
					.getColumnDef(c.getDataType(), c.getColumnSize(), c.isNullable()));
			};
			String create = Util.CREATE_TABLE.replace(TABLE, eq.getKey())
					.replace("[col_def]", sb.toString());
			q.setCreateQuery(create);
		}
	}

}
