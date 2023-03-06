package com.jitin.kafka.demo.b.customdeserializer;

public class Order {
	private String customerName;
	private String prouct;
	private int quantity;

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

	@Override
	public String toString() {
		return "Order [customerName=" + customerName + ", prouct=" + prouct + ", quantity=" + quantity + "]";
	}

}
