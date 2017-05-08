package com.gaintelectronicbrain.hadoop.dao;

import java.util.List;

import com.gaintelectronicbrain.hadoop.model.Employee;

public interface EmployeeDAO {
	
    public void create(Employee e);
	
	public void update(Employee e);
	
	public void deleteById(String employeeId);	
	
	public Employee findByEmployeeId(String employeeId);
	
	public List<Employee> findAll();

}
