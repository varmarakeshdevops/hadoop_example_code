package com.gaintelectronicbrain.hadoop.model;

public class Employee {
	
	private String employeId;
	private String firstName;
	private String lastName;
	private String email;
	private String mobile;

	public Employee(String employeId, String firstName, String lastName, String email, String mobile) {
		this.employeId = employeId;
		this.firstName = firstName;
		this.lastName = lastName;
		this.email = email;
		this.mobile = mobile;
	}

	public Employee() {
	}

	public String getEmployeId() {
		return employeId;
	}

	public void setEmployeId(String employeId) {
		this.employeId = employeId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	
	@Override
	public String toString(){
		return employeId+" :: "+firstName+ " :: "+lastName+" :: "+email+" :: "+mobile;
	}

}
