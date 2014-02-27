package redis.clients.johm.benchmark;

import org.junit.Test;

import redis.clients.johm.JOhm;
import redis.clients.johm.NVField;
import redis.clients.johm.NVField.Condition;
import redis.clients.johm.models.User;

public class SaveRangeSearchBenchmark extends JOhmBenchmarkTestBase {
	@Test
	public void saveSearchModel() {
		int totalOps = 5000;
		timer.begin();
		User user = null;
		for (int n = 0; n < totalOps; n++) {
			user = new User();
			user.setEmployeeNumber(1);
			user.setDepartmentNumber(2);
			user.setName("foo" + n);
			user.setRoom("vroom" + n);
			user.setAge(n);
			user.setSalary(9999.99f);
			user.setInitial('f');
			JOhm.save(user);
			JOhm.find(User.class, false, new NVField("employeeNumber",1), new NVField("name", "foo" + n));
			JOhm.find(User.class, false, new NVField("employeeNumber",1), new NVField("age", n));
			JOhm.find(User.class, false, new NVField("departmentNumber",2), new NVField("name", "foo" + n));
			JOhm.find(User.class, false, new NVField("departmentNumber",2), new NVField("age", n));
		}
		timer.end();
		printStats("saveSearchModel", totalOps, 5, timer.elapsed());
	}

	@Test
	public void saveRangeSearchModelWithOnlyGreaterThanCondition() {
		int totalOps = 1000;
		timer.begin();
		User user = null;
		for (int n = 0; n < totalOps; n++) {
			user = new User();
			user.setEmployeeNumber(1);
			user.setDepartmentNumber(2);
			user.setName("foo" + n);
			user.setRoom("vroom" + n);
			user.setAge(n);
			user.setSalary(9999.99f + n);
			user.setInitial('f');
			JOhm.save(user);
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("age", n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("salary", 9999.99f + n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("age", n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("salary", 9999.99f + n, Condition.GREATERTHANEQUALTO));
		}
		timer.end();
		printStats("saveRangeSearchModelWithOnlyGreaterThanCondition", totalOps, 5, timer.elapsed());
	}
	
	@Test
	public void saveRangeSearchModelWithGreaterThanCondition() {
		int totalOps = 1000;
		timer.begin();
		User user = null;
		for (int n = 0; n < totalOps; n++) {
			user = new User();
			user.setEmployeeNumber(1);
			user.setDepartmentNumber(2);
			user.setName("foo" + n);
			user.setRoom("vroom" + n);
			user.setAge(n);
			user.setSalary(9999.99f + n);
			user.setInitial('f');
			JOhm.save(user);
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("name", "foo" + n), new NVField("age", n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("name", "foo" + n), new NVField("salary", 9999.99f + n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("name", "foo" + n), new NVField("age", n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("name", "foo" + n), new NVField("salary", 9999.99f + n, Condition.GREATERTHANEQUALTO));
		}
		timer.end();
		printStats("saveRangeSearchModelWithGreaterThanCondition", totalOps, 5, timer.elapsed());
	}
	
	@Test
	public void saveRangeSearchModelWithLessThanCondition() {
		int totalOps = 5000;
		timer.begin();
		User user = null;
		for (int n = 0; n < totalOps; n++) {
			user = new User();
			user.setEmployeeNumber(1);
			user.setDepartmentNumber(2);
			user.setName("foo" + n);
			user.setRoom("vroom" + n);
			user.setAge(n);
			user.setSalary(9999.99f + n);
			user.setInitial('f');
			JOhm.save(user);
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("name", "foo" + n), new NVField("age", n, Condition.LESSTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("name", "foo" + n), new NVField("salary", 9999.99f + n, Condition.LESSTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("name", "foo" + n), new NVField("age", n, Condition.GREATERTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("name", "foo" + n), new NVField("salary", 9999.99f + n, Condition.GREATERTHANEQUALTO));
		}
		timer.end();
		printStats("saveRangeSearchModelWithLessThanCondition", totalOps, 5, timer.elapsed());
	}
	
	@Test
	public void saveRangeSearchModelWithOnlyLessThanCondition() {
		int totalOps = 5000;
		timer.begin();
		User user = null;
		for (int n = 0; n < totalOps; n++) {
			user = new User();
			user.setEmployeeNumber(1);
			user.setDepartmentNumber(2);
			user.setName("foo" + n);
			user.setRoom("vroom" + n);
			user.setAge(n);
			user.setSalary(9999.99f + n);
			user.setInitial('f');
			JOhm.save(user);
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("age", n, Condition.LESSTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("employeeNumber",1), new NVField("salary", 9999.99f + n, Condition.LESSTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("age", n, Condition.LESSTHANEQUALTO));
			JOhm.find(User.class, true, new NVField("departmentNumber",2), new NVField("salary", 9999.99f + n, Condition.LESSTHANEQUALTO));
		}
		timer.end();
		printStats("saveRangeSearchModelWithOnlyLessThanCondition", totalOps, 5, timer.elapsed());
	}
}
