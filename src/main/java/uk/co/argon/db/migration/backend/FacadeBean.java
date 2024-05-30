package uk.co.argon.db.migration.backend;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import uk.co.argon.common.exceptions.HttpException;
import uk.co.argon.db.migration.backend.dao.TableCreationAndPopulationDao;
import uk.co.argon.db.migration.backend.util.DatatypesQueryConstructor;
import uk.co.argon.db.migration.backend.util.Util;
import uk.co.argon.db.migration.backend.vo.Query;

public class FacadeBean implements Facade {
	private static final int THRESHOLD = 500000;
	private final Set<String> listedTables;;
	private final Set<String> removeEntry;
	private ConcurrentHashMap<String, Query> qs = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> popTab = DatatypesQueryConstructor.getInstance().getPopulateTableOrder();
	
	private TableCreationAndPopulationDao dao;

	public FacadeBean(TableCreationAndPopulationDao dao) {
		this.dao = dao;
		this.listedTables = new HashSet<>();
		this.removeEntry = new HashSet<>();
	}

	@Override
	public void createTableCreationOrder(Set<String> tables) throws HttpException {
		Stack<String> stack = new Stack<>();
		
		for(String table: tables) {
			stack.add(table);
			getOrder(stack);
		}
	}
	
	public void getOrder(Stack<String> stack) {
		if(listedTables.contains(stack.peek())) {
			stack.pop();
			return;
		}
		
		if(DatatypesQueryConstructor.getInstance().getTableDependancies().get(stack.peek()) != null) {
			for(String table: DatatypesQueryConstructor.getInstance().getTableDependancies().get(stack.peek())) {
				stack.add(table);
				getOrder(stack);
			}
		}
		
		DatatypesQueryConstructor.getInstance().addTableToOder(stack.peek());
		listedTables.add(stack.pop());
	}

	@Override
	public void createTables(ConcurrentHashMap<String, Query> queries) throws SQLException {
		System.out.println("\n" + "=".repeat(15) + " Creating Synapse tables " + "=".repeat(15));
		dao.createTablesInSynapse(queries);
		System.out.println("=".repeat(55) + "\n");
	}

	@Override
	public void createTablePopulationOrder(Set<String> tables) {
		listedTables.clear();
		tables.forEach(t-> createPopulationOrder(t));
	}
	
	private void createPopulationOrder(String table) {
		if(popTab.containsKey(table)) return;
		
		ConcurrentHashMap.KeySetView<String, Boolean> dependencies = DatatypesQueryConstructor.getInstance().getTableDependancies().get(table);
		if(dependencies != null) {
			dependencies.forEach(t -> {
				createPopulationOrder(t);
				ConcurrentHashMap.KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet();
				set.add(t);
				set.addAll(popTab.getOrDefault(t, ConcurrentHashMap.newKeySet()));
				set.addAll(popTab.getOrDefault(table, ConcurrentHashMap.newKeySet()));
				popTab.put(table, set);
			});

			popTab.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet());
		}
	}

	@Override
	public void updateInsertionQueries(ConcurrentHashMap<String, Query> queries) throws HttpException {
		
		System.out.println("\n" + "=".repeat(15) + " Spliting large tables " + "=".repeat(15));
		for(Entry<String, Query> e: queries.entrySet()) {
			String table =e.getKey();
			Query q = e.getValue();

			if((q.getEnd() - q.getStart())>THRESHOLD) {
				System.out.println("Spliting: " + table + " calculation: " + (q.getEnd() - q.getStart()));
				splitQuery(e);
				removeEntry.add(table);
			}
		}
		
		queries.putAll(qs);
		updateDependenciesTableValues();
		removeEntry.forEach(s -> queries.remove(s));
		System.out.println("=".repeat(53) + "\n");
	}

	private void splitQuery(Entry<String, Query> e) throws HttpException {
		String append = " t.id >= ? AND t.id <= ?";
		String t = e.getKey();
		Query q = e.getValue();
		int start = q.getStart();
		int count = (q.getEnd() - q.getStart() + 1)/Util.CORE_POOL_SIZE;
		
		for(int i=0; i<Util.CORE_POOL_SIZE; i++) {
			Query q1 = new Query(q);
			String str = (StringUtils.containsIgnoreCase(q.getSelectQuery(), "WHERE")? " AND":" t WHERE") + append;
			q1.setSelectQuery(q.getSelectQuery() + str);
			q1.setEnd(start + count);
			q1.setStart(start);
			qs.put(t+i, q1);
			start = q1.getEnd() + 1;
			updateDependenciesTableKeys(t, t+i);
		}
	}
	
	private void updateDependenciesTableKeys(String oldTable, String newTable) throws HttpException {
		ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> deps = DatatypesQueryConstructor.getInstance().getTableDependancies();
		deps.put(newTable, deps.get(oldTable));
	}
	
	private void updateDependenciesTableValues() throws HttpException {
		ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<String, Boolean>> deps = DatatypesQueryConstructor.getInstance().getTableDependancies();
		
		deps.forEach((k,v) -> {
			removeEntry.forEach(t -> {
				if(v.contains(t))
					for(int i=0; i<Util.CORE_POOL_SIZE; i++)
						v.add(t+i);

				v.remove(t);
			});
		});
		
		removeEntry.forEach(s -> deps.remove(s));
	}
}
