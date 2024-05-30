package uk.co.argon.db.migration.backend.vo;

import java.util.ArrayList;
import java.util.List;

public class Query {
	private int end;
	private int start;
	private String createQuery;
	private String selectQuery;
	private String insertQuery;
	private List<Column> columns;
	private Constraint constraints;

	public Query() {
	}

	public Query(String selectQuery) {
		this.selectQuery = selectQuery;
	}
	
	public Query(String dropQuery, String createQuery, String selectQuery, String insertQuery, int start, int end,
			List<Column> columns, Constraint constraints) {
		this.createQuery = createQuery;
		this.selectQuery = selectQuery;
		this.insertQuery = insertQuery;
		this.start = start;
		this.end = end;
		this.columns = columns;
		this.constraints = constraints;
	}

	public Query(Query q) {
		this.createQuery = q.getCreateQuery();
		this.selectQuery = q.getSelectQuery();
		this.insertQuery = q.getInsertQuery();
		this.start = q.getStart();
		this.end = q.getEnd();
		this.columns = q.getColumns();
		this.constraints = q.getConstraints();
	}

	public List<Column> getColumns() {
		if(columns == null)
			columns = new ArrayList<Column>();
		return columns;
	}
	
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public String getCreateQuery() {
		return createQuery;
	}

	public void setCreateQuery(String createQuery) {
		this.createQuery = createQuery;
	}

	public String getSelectQuery() {
		return selectQuery;
	}

	public void setSelectQuery(String selectQuery) {
		this.selectQuery = selectQuery;
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public Constraint getConstraints() {
		return constraints;
	}

	public void setConstraints(Constraint constraints) {
		this.constraints = constraints;
	}
}
