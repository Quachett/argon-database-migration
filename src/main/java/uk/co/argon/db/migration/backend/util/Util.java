package uk.co.argon.db.migration.backend.util;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;

public class Util {
	public static final String FILE_PATH = "C:/Users/lloyd/OneDrive/dir/mfdb/";
	public static final String ERR_FILE = "error.csv";
	public static final String BIGINT = "bigint";
	public static final String VARCHAR = "varchar";
	public static final String INT = "int";
	public static final String CHAR = "char";
	public static final String DATETIME = "datetime";
	public static final String FLOAT = "float";
	public static final String TINYINT = "tinyint";
	public static final String LONGTEXT = "longtext";
	public static final String TEXT = "text";
	public static final String BIT = "bit";
	public static final String END = "tableEndIndex";
	public static final String START = "tableStartIndex";

	public static final int CORE_POOL_SIZE = 8;
	public static final int MAX_POOL_SIZE = 16;
	public static final int KEEP_ALIVE_TIME = 30;
	public static final int ENABLE = 1;
	public static final int DISABLE = 0;
	
	public static String COMMA = ",";
	public static String COLLATION = "COLLATE utf8mb4_0900_ai_ci";
	public static String SELECT_STATEMENT = "SELECT * FROM MFDB.dbo.[table]";
	public static String INSERT_STATEMENT = "INSERT INTO MFDB1.[table] ([columns]) VALUES ([values])";
	public static String PK_CONSTRAINT = "ALTER TABLE MFDB1.[table] MODIFY `[pkcolumn_name]` INT UNSIGNED NOT NULL AUTO_INCREMENT";
	public static String FK_CONSTRAINT = "ALTER TABLE MFDB1.[table] ADD CONSTRAINT [fk_name] FOREIGN KEY (`[fkcolumn_name]`) REFERENCES MFDB1.[pktable_name](`[pkcolumn_name]`)";
	public static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS MFDB1.[table]([col_def] CONSTRAINT PK_BOOKING_USER PRIMARY KEY (`id`))ENGINE=InnoDB ROW_FORMAT = Dynamic";
	
	public static String getQuestionMarks(int count) {
		return clean("?,".repeat(count));
	}
	
	public static String clean(String str) {
		return StringUtils.substringBeforeLast(str, ",");
	}
	
	public static String getTextField(int size) {
		if(size<4001)
			return "varchar";
		else if(size < 65535)
			return "text";
		else
			return "longtext";
	}
	
	public static String toCamelCase(String str) {
		str = Arrays.toString(str.split("(?=([0-9]{2,}|[A-Z]{2,}|[A-Z][a-z]))"))
				.replace("[","").replace("]", "").replace(",", "");
		return CaseUtils.toCamelCase(str, false, ' ');
	}
	
	public static String twoYearsAgo() {
		return LocalDateTime.now().minusMonths(29).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
	}

}
