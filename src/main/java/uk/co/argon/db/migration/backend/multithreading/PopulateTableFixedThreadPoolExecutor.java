package uk.co.argon.db.migration.backend.multithreading;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PopulateTableFixedThreadPoolExecutor extends ThreadPoolExecutor {
	
	private final ConcurrentHashMap.KeySetView<PopulateTableRunnable, Boolean> pendingTasks;

	public PopulateTableFixedThreadPoolExecutor(int corePoolSize,
			int maximumPoolSize,
			long keepAliveTime,
			TimeUnit unit,
			BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		this.pendingTasks = ConcurrentHashMap.newKeySet();
	}
	
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		((PopulateTableRunnable)r).markAsRun();
		executePendingTasks();
		
		if(t != null)
			t.printStackTrace();

		if(pendingTasks.isEmpty())
			shutdown();
	}
	
	@Override
	public void execute(Runnable r) {
		PopulateTableRunnable task = (PopulateTableRunnable)r;
		ConcurrentHashMap.KeySetView<PopulateTableRunnable, Boolean> dependencies = task.getDependencies();
		
		
		if(!dependencies.stream().allMatch(PopulateTableRunnable::hasRun)) {
			pendingTasks.add(task);
			return;
		}
		else
			super.execute(r);
	}
	
	private synchronized void executePendingTasks() {
		pendingTasks.removeIf(t -> {
			if(((PopulateTableRunnable) t).getDependencies().stream().allMatch(PopulateTableRunnable::hasRun)) {
				super.execute(t);
				return true;
			}
			return false;
		});
	}
}
