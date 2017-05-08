package com.gaintelectronicbrain.hadoop.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.gaintelectronicbrain.hadoop.dao.EmployeeDAO;
import com.gaintelectronicbrain.hadoop.model.Employee;

@RestController
@RequestMapping(value="/employee")
public class EmployeeController {
	
	@Autowired
	EmployeeDAO employeeDAO	;
	
	@RequestMapping(value = "/create", method = RequestMethod.POST, headers="Accept=application/json")
	Map<String, Object> create(@RequestBody Employee employee) {
		employeeDAO.create(employee);
        Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("message", "Employee created successfully");
		dataMap.put("status", "1");
		dataMap.put("employee", employee);
	    return dataMap;
    }	
 
    @RequestMapping(value = "{id}", method = RequestMethod.DELETE, headers="Accept=application/json")
    Map<String, Object> delete(@PathVariable("id") String id) {
        employeeDAO.deleteById(id);
       	Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("message", "Employee deleted successfully");
		dataMap.put("status", "1");
	    return dataMap;
    }
 
    @RequestMapping(value = "/all", method = RequestMethod.GET, headers="Accept=application/json")
    Map<String, Object> findAll() {
    	List<Employee> employees = employeeDAO.findAll();
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("message", "Employees found successfully");
		dataMap.put("totalEmployee", employees.size());
		dataMap.put("status", "1");
		dataMap.put("employees", employees);
	    return dataMap;
    }

    @RequestMapping(value = "{id}", method = RequestMethod.GET, headers="Accept=application/json")
    Map<String, Object> findById(@PathVariable("id") String id) {
        Employee employee=  employeeDAO.findByEmployeeId(id);
        Map<String, Object> dataMap = new HashMap<String, Object>();
        if(employee != null){
        	dataMap.put("message", "Employee found successfully");
			dataMap.put("employee", employee);
        }else{
        	dataMap.put("message", "No Employee found with employeID :" + id);
        }
        dataMap.put("status", "1");
	    return dataMap;
    }
 
    @RequestMapping(value = "/update", method = RequestMethod.PUT, headers="Accept=application/json")
    Map<String, Object> update(@RequestBody Employee employee) {
        employeeDAO.update(employee);
        Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("message", "Employee updated successfully");
		dataMap.put("status", "1");
		dataMap.put("employee", employee);
	    return dataMap;
    }	

}
