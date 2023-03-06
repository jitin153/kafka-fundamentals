package com.jitin.kafka.demo.b.customserializer;

public class Order {
	private String customerName;
	private String prouct;
	private int quantity;

	public Order(String customerName, String prouct, int quantity) {
		this.customerName = customerName;
		this.prouct = prouct;
		this.quantity = quantity;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getProuct() {
		return prouct;
	}

	public void setProuct(String prouct) {
		this.prouct = prouct;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

}
