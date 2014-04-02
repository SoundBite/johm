package redis.clients.johm;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import redis.clients.johm.NVField.Condition;
import redis.clients.johm.models.Address;
import redis.clients.johm.models.Country;
import redis.clients.johm.models.Item;
import redis.clients.johm.models.User;

public class SearchTest extends JOhmTestBase {
	@Test(expected = JOhmException.class)
	public void cannotSearchOnNullField() {
		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setName("model1");
		user1.setRoom("tworoom");
		user1.setAge(88);
		JOhm.save(user1);

		JOhm.find(User.class, null, "foo", JOhm.getHashTag("employeeNumber", String.valueOf(user1.getEmployeeNumber())));
	}

	@Test(expected = JOhmException.class)
	public void cannotSearchWithNullValue() {
		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setName("model1");
		user1.setRoom("tworoom");
		user1.setAge(88);
		JOhm.save(user1);

		JOhm.find(User.class, "age", null, JOhm.getHashTag("employeeNumber", String.valueOf(user1.getEmployeeNumber())));
	}

	@Test(expected = JOhmException.class)
	public void cannotSearchWithOnNotIndexedFields() {
		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setName("model1");
		user1.setRoom("tworoom");
		user1.setAge(88);
		JOhm.save(user1);

		JOhm.find(User.class, "room", "tworoom", JOhm.getHashTag("employeeNumber", String.valueOf(user1.getEmployeeNumber())));
	}

	@Test
	public void checkModelSearch() {
		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("model1");
		user1.setRoom("tworoom");
		user1.setAge(88);
		user1.setSalary(9999.99f);
		user1.setInitial('m');
		JOhm.save(user1);
		Long id1 = user1.getId();

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("zmodel2");
		user2.setRoom("threeroom");
		user2.setAge(8);
		user2.setInitial('z');
		user2 = JOhm.save(user2);
		Long id2 = user2.getId();

		assertNotNull(JOhm.get(User.class, id1));
		assertNotNull(JOhm.get(User.class, id2));

		List<User> users = JOhm.find(User.class, "age", 88, JOhm.getHashTag("employeeNumber", "1"));
		assertEquals(1, users.size());
		User user1Found = users.get(0);
		assertEquals(user1Found.getAge(), user1.getAge());
		assertEquals(user1Found.getName(), user1.getName());
		assertNull(user1Found.getRoom());
		assertEquals(user1Found.getSalary(), user1.getSalary(), 0D);
		assertEquals(user1Found.getInitial(), user1.getInitial());
		
		users = JOhm.find(User.class, "age", 88, JOhm.getHashTag("departmentNumber", "2"));
		assertEquals(1, users.size());
		user1Found = users.get(0);
		assertEquals(user1Found.getAge(), user1.getAge());
		assertEquals(user1Found.getName(), user1.getName());
		assertNull(user1Found.getRoom());
		assertEquals(user1Found.getSalary(), user1.getSalary(), 0D);
		assertEquals(user1Found.getInitial(), user1.getInitial());

		users = JOhm.find(User.class, "age", 8, JOhm.getHashTag("employeeNumber", "1"));
		assertEquals(1, users.size());
		User user2Found = users.get(0);
		assertEquals(user2Found.getAge(), user2.getAge());
		assertEquals(user2Found.getName(), user2.getName());
		assertNull(user2Found.getRoom());
		assertEquals(user2Found.getSalary(), user2.getSalary(), 0D);
		assertEquals(user2Found.getInitial(), user2.getInitial());
		
		users = JOhm.find(User.class, "age", 8, JOhm.getHashTag("departmentNumber", "2"));
		assertEquals(1, users.size());
		user2Found = users.get(0);
		assertEquals(user2Found.getAge(), user2.getAge());
		assertEquals(user2Found.getName(), user2.getName());
		assertNull(user2Found.getRoom());
		assertEquals(user2Found.getSalary(), user2.getSalary(), 0D);
		assertEquals(user2Found.getInitial(), user2.getInitial());

		users = JOhm.find(User.class, "name", "model1", JOhm.getHashTag("employeeNumber", "1"));
		assertEquals(1, users.size());
		User user3Found = users.get(0);
		assertEquals(user3Found.getAge(), user1.getAge());
		assertEquals(user3Found.getName(), user1.getName());
		assertNull(user3Found.getRoom());
		assertEquals(user3Found.getSalary(), user1.getSalary(), 0D);
		assertEquals(user3Found.getInitial(), user1.getInitial());
		
		users = JOhm.find(User.class, "name", "model1", JOhm.getHashTag("departmentNumber", "2"));
		assertEquals(1, users.size());
		user3Found = users.get(0);
		assertEquals(user3Found.getAge(), user1.getAge());
		assertEquals(user3Found.getName(), user1.getName());
		assertNull(user3Found.getRoom());
		assertEquals(user3Found.getSalary(), user1.getSalary(), 0D);
		assertEquals(user3Found.getInitial(), user1.getInitial());

		users = JOhm.find(User.class, "name", "zmodel2", JOhm.getHashTag("employeeNumber", "1"));
		assertEquals(1, users.size());
		User user4Found = users.get(0);
		assertEquals(user4Found.getAge(), user2.getAge());
		assertEquals(user4Found.getName(), user2.getName());
		assertNull(user4Found.getRoom());
		assertEquals(user4Found.getSalary(), user2.getSalary(), 0D);
		assertEquals(user4Found.getInitial(), user2.getInitial());
		
		users = JOhm.find(User.class, "name", "zmodel2", JOhm.getHashTag("departmentNumber", "2"));
		assertEquals(1, users.size());
		user4Found = users.get(0);
		assertEquals(user4Found.getAge(), user2.getAge());
		assertEquals(user4Found.getName(), user2.getName());
		assertNull(user4Found.getRoom());
		assertEquals(user4Found.getSalary(), user2.getSalary(), 0D);
		assertEquals(user4Found.getInitial(), user2.getInitial());
	}

	@Test
	public void canSearchOnLists() {
		Item item = new Item();
		item.setName("bar");
		JOhm.save(item);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("foo");
		JOhm.save(user1);
		user1.getLikes().add(item);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("car");
		JOhm.save(user2);
		user2.getLikes().add(item);

		List<User> users = JOhm.find(User.class, "likes", item.getId(), JOhm.getHashTag("employeeNumber", "1"));

		assertEquals(2, users.size());
		Long[] controlSet = { user1.getId(), user2.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
		
		users = JOhm.find(User.class, "likes", item.getId(), JOhm.getHashTag("departmentNumber", "2"));

		assertEquals(2, users.size());
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}
	
	@Test
	public void canSearchOnArrays() {
		Item item0 = new Item();
		item0.setName("Foo0");
		JOhm.save(item0);

		Item item1 = new Item();
		item1.setName("Foo1");
		JOhm.save(item1);

		Item item2 = new Item();
		item2.setName("Foo2");
		JOhm.save(item2);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("foo");
		user1.setThreeLatestPurchases(new Item[] { item0, item1, item2 });
		JOhm.save(user1);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("car");
		JOhm.save(user2);

		List<User> users = JOhm.find(User.class, "threeLatestPurchases", item0.getId(), JOhm.getHashTag("employeeNumber", "1"));
		assertEquals(1, users.size());
		assertEquals(user1.getId(), users.get(0).getId());

		User user3 = new User();
		user3.setEmployeeNumber(1);
		user3.setDepartmentNumber(2);
		user3.setName("foo");
		user3.setThreeLatestPurchases(new Item[] { item0, item1, item2 });
		JOhm.save(user3);

		users = JOhm.find(User.class, "threeLatestPurchases", item0.getId(), JOhm.getHashTag("departmentNumber", "2"));
		assertEquals(2, users.size());

		Long[] controlSet = { user1.getId(), user3.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}

	@Test
	public void canSearchOnSets() {
		Item item = new Item();
		item.setName("bar");
		JOhm.save(item);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("foo");
		JOhm.save(user1);
		user1.getPurchases().add(item);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("car");
		JOhm.save(user2);
		user2.getPurchases().add(item);

		List<User> users = JOhm.find(User.class, "purchases", item.getId(), JOhm.getHashTag("employeeNumber", "1"));

		assertEquals(2, users.size());
		Long[] controlSet = { user1.getId(), user2.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);

		users = JOhm.find(User.class, "purchases", item.getId(), JOhm.getHashTag("departmentNumber", "1"));

		assertEquals(2, users.size());
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}

	@Test
	public void canSearchOnSortedSets() {
		Item item = new Item();
		item.setName("bar");
		JOhm.save(item);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("foo");
		JOhm.save(user1);
		user1.getOrderedPurchases().add(item);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("car");
		JOhm.save(user2);
		user2.getOrderedPurchases().add(item);

		List<User> users = JOhm.find(User.class, "orderedPurchases", item
				.getId(), JOhm.getHashTag("employeeNumber", "1"));

		assertEquals(2, users.size());
		Long[] controlSet = { user1.getId(), user2.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
		
		users = JOhm.find(User.class, "orderedPurchases", item
				.getId(), JOhm.getHashTag("departmentNumber", "2"));

		assertEquals(2, users.size());
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}

	@Test
	public void canSearchOnMaps() {
		Item item = new Item();
		item.setName("bar");
		JOhm.save(item);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setName("foo");
		JOhm.save(user1);
		user1.getFavoritePurchases().put(1, item);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setName("car");
		JOhm.save(user2);
		user2.getFavoritePurchases().put(1, item);

		List<User> users = JOhm.find(User.class, "favoritePurchases", item
				.getId(), JOhm.getHashTag("employeeNumber", "1"));

		assertEquals(2, users.size());
		Long[] controlSet = { user1.getId(), user2.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);

		users = JOhm.find(User.class, "favoritePurchases", item
				.getId(), JOhm.getHashTag("departmentNumber", "2"));

		assertEquals(2, users.size());
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}

	@Test
	public void canSearchOnReferences() {
		Country somewhere = new Country();
		somewhere.setName("somewhere");
		JOhm.save(somewhere);

		User user1 = new User();
		user1.setEmployeeNumber(1);
		user1.setDepartmentNumber(2);
		user1.setCountry(somewhere);
		JOhm.save(user1);

		User user2 = new User();
		user2.setEmployeeNumber(1);
		user2.setDepartmentNumber(2);
		user2.setCountry(somewhere);
		JOhm.save(user2);

		List<User> users = JOhm.find(User.class, "country", somewhere.getId(), JOhm.getHashTag("employeeNumber", "1"));

		assertEquals(2, users.size());
		Long[] controlSet = { user1.getId(), user2.getId() };
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
		
		users = JOhm.find(User.class, "country", somewhere.getId(), JOhm.getHashTag("departmentNumber", "2"));

		assertEquals(2, users.size());
		assertTrue(users.get(0).getId() != users.get(1).getId());
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(0).getId()) >= 0);
		assertTrue(Arrays.binarySearch(controlSet, 0, controlSet.length, users.get(1).getId()) >= 0);
	}

	@Test
	public void cannotSearchAfterDeletingIndexes() {
		User user = new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		JOhm.save(user);

		user.setAge(77); // younger
		JOhm.save(user);

		user.setAge(66); // younger still
		JOhm.save(user);

		Long id = user.getId();

		assertNotNull(JOhm.get(User.class, id));

		String hashTagBasedOnEmployee = JOhm.getHashTag("employeeNumber", String.valueOf(user.getEmployeeNumber()));
		List<User> users = JOhm.find(User.class, "age", 88, hashTagBasedOnEmployee);
		assertEquals(0, users.size()); // index already updated
		users = JOhm.find(User.class, "age", 77, hashTagBasedOnEmployee);
		assertEquals(0, users.size()); // index already updated
		users = JOhm.find(User.class, "age", 66, hashTagBasedOnEmployee);
		assertEquals(1, users.size());

		String hashTagBasedOnDepartment = JOhm.getHashTag("departmentNumber", String.valueOf(user.getDepartmentNumber()));
		users = JOhm.find(User.class, "age", 88, hashTagBasedOnDepartment);
		assertEquals(0, users.size()); // index already updated
		users = JOhm.find(User.class, "age", 77, hashTagBasedOnDepartment);
		assertEquals(0, users.size()); // index already updated
		users = JOhm.find(User.class, "age", 66, hashTagBasedOnDepartment);
		assertEquals(1, users.size());
		
		JOhm.delete(User.class, id);

		users = JOhm.find(User.class, "age", 88, hashTagBasedOnEmployee);
		assertEquals(0, users.size());
		users = JOhm.find(User.class, "age", 77, hashTagBasedOnEmployee);
		assertEquals(0, users.size());
		users = JOhm.find(User.class, "age", 66, hashTagBasedOnEmployee);
		assertEquals(0, users.size());

		users = JOhm.find(User.class, "age", 88, hashTagBasedOnDepartment);
		assertEquals(0, users.size());
		users = JOhm.find(User.class, "age", 77, hashTagBasedOnDepartment);
		assertEquals(0, users.size());
		users = JOhm.find(User.class, "age", 66, hashTagBasedOnDepartment);
		assertEquals(0, users.size());
		
		assertNull(JOhm.get(User.class, id));
	}

	@Test
	public void multiFindWithDifferentHashTagValues() {
		User user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		JOhm.save(user);

		user=new User();     
		user.setEmployeeNumber(2);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("f");
		JOhm.save(user);

		List<User> gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age",88));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age",88));
		assertEquals(2,gotUsers.size());

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(3);
		user.setAge(88);
		user.setName("d");
		JOhm.save(user);

		gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age",88));
		assertEquals(2,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",3), new NVField("age",88));
		assertEquals(1,gotUsers.size());
	}

	@Test
	public void canDoMultiFind() {
		Address someWhereAddress = new Address();
		someWhereAddress.setStreetName("xyz");
		JOhm.save(someWhereAddress);

		Country somewhere = new Country();
		somewhere.setName("somewhere");
		JOhm.save(somewhere);

		User user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		user.setSalary(2000f);
		user.setAddress(someWhereAddress);
		user.setCountry(somewhere);
		JOhm.save(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(1000f);
		user.setName("f");
		JOhm.save(user);

		user=new User();     
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setSalary(3000f);
		user.setName("f");
		user.setCountry(somewhere);
		JOhm.save(user);

		user=new User(); 
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(2000f);
		user.setName("b");
		JOhm.save(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(49);
		user.setSalary(6000f);
		user.setName("m");
		JOhm.save(user);

		List<User> gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age",88), new NVField("name","b"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals("b",gotUsers.get(0).getName());

		gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age", 88), new NVField("country","name", "somewhere"));
		assertEquals(2,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age", 88), new NVField("country","name", "somewhere"), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals(somewhere,gotUsers.get(0).getCountry());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age",88), new NVField("name","b"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals("b",gotUsers.get(0).getName());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age", 88), new NVField("country","name", "somewhere"));
		assertEquals(2,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age", 88), new NVField("country","name", "somewhere"), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals(somewhere,gotUsers.get(0).getCountry());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
	}

	@Test
	public void canDoMultiFindWithTransactedSave() {
		Address someWhereAddress = new Address();
		someWhereAddress.setStreetName("xyz");
		JOhm.transactedSave(someWhereAddress);

		Country somewhere = new Country();
		somewhere.setName("somewhere");
		JOhm.transactedSave(somewhere);

		User user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		user.setSalary(2000f);
		user.setAddress(someWhereAddress);
		user.setCountry(somewhere);
		JOhm.transactedSave(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(1000f);
		user.setName("f");
		JOhm.transactedSave(user);

		user=new User();    
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setSalary(3000f);
		user.setName("f");
		user.setCountry(somewhere);
		JOhm.transactedSave(user);

		user=new User();  
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(2000f);
		user.setName("b");
		JOhm.transactedSave(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(49);
		user.setSalary(6000f);
		user.setName("m");
		JOhm.transactedSave(user);

		List<User> gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age",88), new NVField("name","b"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals("b",gotUsers.get(0).getName());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88), new NVField("country","name", "somewhere"));
		assertEquals(2,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88), new NVField("country","name", "somewhere"), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals(somewhere,gotUsers.get(0).getCountry());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
		
		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age",88), new NVField("name","b"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals("b",gotUsers.get(0).getName());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88), new NVField("country","name", "somewhere"));
		assertEquals(2,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88), new NVField("country","name", "somewhere"), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
		assertEquals(88,gotUsers.get(0).getAge());
		assertEquals(somewhere,gotUsers.get(0).getCountry());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("address","streetName", "xyz"));
		assertEquals(1,gotUsers.size());
	}

	@Test
	public void canDoMultiFindWithRangeConditions() {
		Address someWhereAddress = new Address();
		someWhereAddress.setStreetName("xyz");
		someWhereAddress.setHouseNumber(12);
		JOhm.save(someWhereAddress);

		Country somewhere = new Country();
		somewhere.setName("somewhere");
		JOhm.save(somewhere);

		User user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		user.setSalary(2000f);
		user.setAddress(someWhereAddress);
		user.setCountry(somewhere);
		JOhm.save(user);

		Long idOfUser1 = user.getId();

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(1000f);
		user.setName("f");
		user.setCountry(somewhere);
		JOhm.save(user);

		user=new User();  
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setSalary(3000f);
		user.setName("f");
		JOhm.save(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(2000f);
		user.setName("b");
		JOhm.save(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(49);
		user.setSalary(6000f);
		user.setName("m");
		JOhm.save(user);

		List<User> gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO));
		assertEquals(4,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHAN), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 3000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 3000, Condition.GREATERTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHAN), new NVField("country","name", "somewhere"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", "12"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
		assertEquals(gotUsers.get(0).getId(), idOfUser1);
		
		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO));
		assertEquals(4,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHAN), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 3000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 3000, Condition.GREATERTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHAN), new NVField("country","name", "somewhere"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", "12"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
		assertEquals(gotUsers.get(0).getId(), idOfUser1);
		
		JOhm.delete(User.class, idOfUser1);
		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(0,gotUsers.size());
		
		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(0,gotUsers.size());
		
		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		user.setSalary(2000f);
		user.setAddress(someWhereAddress);
		user.setCountry(somewhere);
		JOhm.save(user);

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
		
		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
	}

	@Test
	public void canDoMultiFindWithRangeConditionsWithTransactedSave() {
		Address someWhereAddress = new Address();
		someWhereAddress.setStreetName("xyz");
		someWhereAddress.setHouseNumber(12);
		JOhm.transactedSave(someWhereAddress);

		Country somewhere = new Country();
		somewhere.setName("somewhere");
		JOhm.transactedSave(somewhere);

		User user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setName("b");
		user.setSalary(2000f);
		user.setAddress(someWhereAddress);
		user.setCountry(somewhere);
		JOhm.transactedSave(user);

		Long idOfUser1 = user.getId();

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(1000f);
		user.setName("f");
		user.setCountry(somewhere);
		JOhm.transactedSave(user);

		user=new User();   
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(88);
		user.setSalary(3000f);
		user.setName("f");
		JOhm.transactedSave(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(55);
		user.setSalary(2000f);
		user.setName("b");
		JOhm.transactedSave(user);

		user=new User();
		user.setEmployeeNumber(1);
		user.setDepartmentNumber(2);
		user.setAge(49);
		user.setSalary(6000f);
		user.setName("m");
		JOhm.transactedSave(user);

		List<User> gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,  new NVField("employeeNumber",1), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false,  new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false,  new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO));
		assertEquals(4,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHAN), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 3000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("salary", 3000, Condition.GREATERTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHAN), new NVField("country","name", "somewhere"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", "12"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
		assertEquals(gotUsers.get(0).getId(), idOfUser1);
		
		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO));
		assertEquals(4,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.LESSTHANEQUALTO));
		assertEquals(2,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.GREATERTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO));
		assertEquals(3,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("departmentNumber",2), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 1000, Condition.LESSTHANEQUALTO), new NVField("age", 55, Condition.GREATERTHANEQUALTO), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.EQUALS), new NVField("name","f"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 2000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHAN), new NVField("name","f"));
		assertEquals(0,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 3000, Condition.LESSTHAN), new NVField("age", 55, Condition.LESSTHANEQUALTO), new NVField("name","b"));
		assertEquals(1,gotUsers.size());

		gotUsers = JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("salary", 3000, Condition.GREATERTHAN));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHAN), new NVField("country","name", "somewhere"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", "12"));
		assertEquals(1,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(1,gotUsers.size());
		assertEquals(gotUsers.get(0).getId(), idOfUser1);

		JOhm.delete(User.class, idOfUser1);
		
		gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(0,gotUsers.size());

		gotUsers=JOhm.find(User.class,false,new NVField("employeeNumber",1), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name", "b"));
		assertEquals(0,gotUsers.size());
		
		gotUsers=JOhm.find(User.class,false,new NVField("departmentNumber",2),new NVField("age", 88, Condition.LESSTHANEQUALTO), new NVField("address","houseNumber", 12, Condition.LESSTHANEQUALTO));
		assertEquals(0,gotUsers.size());

		gotUsers=JOhm.find(User.class,false, new NVField("departmentNumber",2), new NVField("age", 88, Condition.GREATERTHANEQUALTO), new NVField("name", "b"));
		assertEquals(0,gotUsers.size());
	}
}