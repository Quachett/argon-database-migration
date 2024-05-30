package uk.co.argon.db.migration.backend.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import uk.co.argon.common.exceptions.HttpException;

public class DatatypesQueryConstructor {
	private static DatatypesQueryConstructor instance;
	private final List<String> tableOrder;
	private final Map<String, String> datatypes;
	private final ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> tableDependancies;
	private ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> populateTableOrder;
	
	private DatatypesQueryConstructor() {
		this.datatypes = new HashMap<>();
		populateDatatypesMap();
		this.tableDependancies = new ConcurrentHashMap<>();
		this.tableOrder = new ArrayList<>();
		this.populateTableOrder = new ConcurrentHashMap<>();
	}
	
	public static DatatypesQueryConstructor getInstance() {
		if(instance == null) {
			synchronized (DatatypesQueryConstructor.class) {
				if(instance == null) {
					instance = new DatatypesQueryConstructor();
				}
			}
		}
		return instance;
	}
	
	public String getMapping(String str) {
		return datatypes.get(str);
	}
	
	public String getColumnDef(String type, int size, boolean nullable) throws HttpException {
		
		StringBuilder sb = new StringBuilder();
		String nullStr = (nullable)? "NULL": "NOT NULL";
		
		switch (type) {
		case "int":
		case "bigint":
			sb.append(datatypes.get(type))
			.append(StringUtils.SPACE)
			.append("UNSIGNED").append(StringUtils.SPACE)
			.append(nullStr).append(Util.COMMA);
			break;

		case "bit":
		case "tinyint":
			sb.append(datatypes.get(type))
			.append("(" + size + ")").append(StringUtils.SPACE)
			.append(nullStr).append(Util.COMMA);
			break;

		case "float":
		case "datetime":
		case "timestamp":
			sb.append(datatypes.get(type))
			.append(StringUtils.SPACE)
			.append(nullStr).append(Util.COMMA);
			break;

		case "text":
		case "longtext":
			sb.append(datatypes.get(type)).append(StringUtils.SPACE)
			.append(Util.COLLATION).append(StringUtils.SPACE)
			.append(nullStr).append(Util.COMMA);
			break;

		case "varchar":
		case "uniqueidentifier":
			sb.append(datatypes.get(type))
			.append("(" + size + ")").append(StringUtils.SPACE)
			.append(Util.COLLATION).append(StringUtils.SPACE)
			.append(nullStr).append(Util.COMMA);
			break;

		default:
			throw new HttpException("Unkown DataType: " + type, 404);
		}
		
		return sb.toString();
	}
	
	private void populateDatatypesMap() {
		datatypes.put("int", "INT");
		datatypes.put("tinyint", "TINYINT");
		datatypes.put("bit", "TINYINT");
		datatypes.put("float", "FLOAT");
		datatypes.put("datetime", "DATETIME(6)");
		datatypes.put("timestamp", "TIMESTAMP");
		datatypes.put("text", "TEXT");
		datatypes.put("longtext", "LONGTEXT");
		datatypes.put("bigint", "BIGINT");
		datatypes.put("varchar", "VARCHAR");
		datatypes.put("uniqueidentifier", "VARCHAR");
	}
	
	public ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> getTableDependancies() {
		return tableDependancies;
	}
	
	public void addTableDependancies(String tableName, ConcurrentHashMap.KeySetView<String, Boolean> uncestors) {
		tableDependancies.put(tableName, uncestors);
	}
	
	public void addTableToOder(String table) {
		tableOrder.add(table);
	}
	
	public List<String> getTableOrder() {
		return tableOrder;
	}

	public ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> getPopulateTableOrder() {
		return populateTableOrder;
	}

	public void setPopulateTableOrder(ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> populateTableOrder) {
		this.populateTableOrder = populateTableOrder;
	}
}
