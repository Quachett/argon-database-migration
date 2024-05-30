package uk.co.argon.db.migration.backend;

import uk.co.argon.db.migration.backend.vo.Query;

public interface PopulateTablesFacade {
	public void populateTable(Query query, int start, int end);
}
