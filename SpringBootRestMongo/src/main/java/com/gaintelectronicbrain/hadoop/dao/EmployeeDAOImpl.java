package com.gaintelectronicbrain.hadoop.dao;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.gaintelectronicbrain.hadoop.model.Employee;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class EmployeeDAOImpl implements EmployeeDAO {

	private MongoOperations mongoOps;
		
	private static final String EMPLOYEE_COLLECTION = "Employee";
	
	private DBCollection getDBCollection(){
		return this.mongoOps.getCollection(EMPLOYEE_COLLECTION);
	}
	
	public EmployeeDAOImpl(MongoOperations mongoOps){
		this.mongoOps=mongoOps;
	}
	
	public void create(Employee e) {
		this.mongoOps.insert(e, EMPLOYEE_COLLECTION);
	}

	public void update(Employee e) {
		DBCollection  coll = getDBCollection();
		DBObject query = new BasicDBObject("employeId", e.getEmployeId());
		//delete existing record.
		coll.findAndRemove(query);
		// insert new object with updates
		this.mongoOps.insert(e, EMPLOYEE_COLLECTION);
	}

	public void deleteById(String employeeId) {
		DBCollection  coll = getDBCollection();
		DBObject query = new BasicDBObject("employeId", employeeId);
		DBObject obj = coll.findAndRemove(query);
	}
	
	public Employee findByEmployeeId(String employeeId) {
	
		DBCollection  coll = getDBCollection();
		// creating new object
		DBObject query = new BasicDBObject("employeId", employeeId);
		DBObject d1 = coll.findOne(query);
		Employee emp =  this.mongoOps.getConverter().read(Employee.class, d1);
		return emp;
	}
	
	public List<Employee> findAll(){
		DBCollection  coll = getDBCollection();
		DBCursor cursor = coll.find();
		List<Employee> empList = new ArrayList<Employee>();	
		while (cursor.hasNext()) { 
		    DBObject obj = cursor.next(); 
		    Employee emp = this.mongoOps.getConverter().read(Employee.class, obj);  
		    empList.add(emp); 
		}
		return empList;
	}

}
