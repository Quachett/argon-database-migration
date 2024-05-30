package uk.co.argon.db.migration.backend.vo;

import java.util.ArrayList;
import java.util.List;

public class Constraint {
	
	private String primaryKey;
	private List<String> foreignKey;
	
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public List<String> getForeignKey() {
		if(foreignKey == null)
			foreignKey = new ArrayList<>();
		return foreignKey;
	}
	public void setForeignKey(List<String> foreignKey) {
		this.foreignKey = foreignKey;
	}
}
