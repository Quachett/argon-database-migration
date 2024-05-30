package uk.co.argon.db.migration.backend.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang3.StringUtils;

import uk.co.argon.db.migration.backend.util.DatatypesQueryConstructor;
import uk.co.argon.db.migration.backend.util.Util;
import uk.co.argon.db.migration.backend.vo.Column;
import uk.co.argon.db.migration.backend.vo.Constraint;
import uk.co.argon.db.migration.backend.vo.Query;

public class TableCreationAndPopulationDaoBean implements TableCreationAndPopulationDao {
	
	private static final String OVAFLO_USERNAME = "argon";
	private static final String OVAFLO_PASSWORD = "Q15_4r90n";
	private static final String OVAFLO_DB_Url = "jdbc:sqlserver://;serverName=localhost;port=1433;databaseName=MFDB;trustServerCertificate=true";
	
	private static final String MYSQL_USERNAME = "argon";
	private static final String MYSQL_PASSWORD = "4r90n";
	private static final String MYSQL_DB_Url = "jdbc:mysql://localhost:3306/MFDB1?allowMultiQueries=true";

	@Override
	public void createTablesInSynapse(Map<String, Query> queries) throws SQLException {
		try(Connection conn = DriverManager.getConnection(MYSQL_DB_Url, MYSQL_USERNAME, MYSQL_PASSWORD)) {
			
			try(PreparedStatement ps = conn.prepareStatement("DROP DATABASE IF EXISTS `MFDB1`;CREATE DATABASE `MFDB1`;")) {
				ps.execute();
			}
			
			for(String table: DatatypesQueryConstructor.getInstance().getTableOrder()) {
				System.out.println("Creating table: " + table);
				try(PreparedStatement ps = conn.prepareStatement(getQuery(table, queries))) {
					ps.execute();
				}
			}
		}
	}
	
	private String getQuery(String table, Map<String, Query> queries) {
		StringBuilder sb = new StringBuilder();
		
		Constraint c = queries.get(table).getConstraints();
		
		sb.append(queries.get(table).getCreateQuery()).append(";")
		.append(c.getPrimaryKey()).append(";");
		
		if(!c.getForeignKey().isEmpty())
			c.getForeignKey().forEach(q -> sb.append(q+";"));
		return sb.toString();
	}
	
	@Override
	public boolean setForeignKeyCheckStatus(int values) {
		try(Connection conn = DriverManager.getConnection(MYSQL_DB_Url, MYSQL_USERNAME, MYSQL_PASSWORD)) {
			
			try(PreparedStatement ps = conn.prepareStatement("SET GLOBAL FOREIGN_KEY_CHECKS=?;")) {
				ps.setInt(1, values);
				ps.executeUpdate();
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void migrateData(Query query, int start, int end) {
		try(Connection conn = DriverManager.getConnection(OVAFLO_DB_Url, OVAFLO_USERNAME, OVAFLO_PASSWORD);
				PreparedStatement ps = conn.prepareStatement(query.getSelectQuery())) {
			
			ps.setFetchDirection(ResultSet.FETCH_FORWARD);
			ps.setFetchSize(10000);

			if(StringUtils.containsIgnoreCase(query.getSelectQuery(), "t.dateAdded")) {
				ps.setString(1, Util.twoYearsAgo());
				if(StringUtils.containsIgnoreCase(query.getSelectQuery(), "t.id")) {
					ps.setInt(2, start);
					ps.setInt(3, end);
				}
			}
			
			try(ResultSet rs = ps.executeQuery()) {
				insertIntoMFDBSynapse(rs, query);
			}
		}
		catch (SQLException e) {
			System.err.println("ERROR: " + query.getSelectQuery() + ": " + e.getMessage());
		}
	}

	private void insertIntoMFDBSynapse(ResultSet rs, Query query) {
		try(Connection conn = DriverManager.getConnection(MYSQL_DB_Url, MYSQL_USERNAME, MYSQL_PASSWORD)) {
			conn.setAutoCommit(false);
			
			try(PreparedStatement ps = conn.prepareStatement(query.getInsertQuery())) {
				int count = 0;
				
				while(rs.next()) {
					setPreparedStatementParameters(ps, query.getColumns(), rs);
					ps.addBatch();
					
					if(++count%10000 == 0)
						ps.executeBatch();
				}
				ps.executeBatch();
				conn.commit();
			}
		}
		catch (SQLException e) {
			System.err.println("ERROR: " + query.getInsertQuery() + ": " + e.getMessage());
		}
	}

	private void setPreparedStatementParameters(PreparedStatement ps, List<Column> columns, ResultSet rs) throws SQLException {
		int i=1;
		for(Column c: columns) {
			switch (c.getDataType().toLowerCase()) {
			case Util.BIGINT:
				ps.setLong(i, rs.getLong(c.getName()));
				break;
				
			case Util.INT:
				ps.setInt(i, rs.getInt(c.getName()));
				break;
				
			case Util.CHAR:
				ps.setString(i, rs.getString(c.getName()));
				break;
				
			case Util.DATETIME:
				ps.setTimestamp(i, rs.getTimestamp(c.getName()));						
				break;
				
			case Util.FLOAT:
				ps.setFloat(i, rs.getFloat(c.getName()));						
				break;

			case Util.LONGTEXT:
			case Util.TEXT:
				String str = rs.getString(c.getName());
				ps.setBlob(i, (StringUtils.isNotBlank(str))? new SerialBlob(str.getBytes()):null);				
				break;

			case Util.BIT:
			case Util.TINYINT:
				ps.setBoolean(i, rs.getInt(c.getName())==1);
				break;
				
			case Util.VARCHAR:
				ps.setString(i, rs.getString(c.getName()));						
				break;

			default:
				throw new IllegalArgumentException("unknown datatype: " + c.getDataType());
			}
			i++;
		}
	}
}
