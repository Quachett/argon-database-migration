package uk.co.argon.db.migration.backend.vo;

public class Column {
	private String name;
	private String dataType;
	private int columnSize;
	private boolean isNullable;
	
	public Column() {
	}

	public Column(String name, String dataType) {
		this.name = name;
		this.dataType = dataType;
	}

	public Column(String name, String dataType, int columnSize, boolean isNullable) {
		this.name = name;
		this.dataType = dataType;
		this.columnSize = columnSize;
		this.isNullable = isNullable;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getDataType() {
		return dataType;
	}
	
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}
}
