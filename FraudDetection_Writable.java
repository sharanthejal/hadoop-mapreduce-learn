package com.hadoop.learn.mr.frauddetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

//Id, name, ordered date, dispatch date, courier, delivered date, returned, returned date, reason
//GGYZ333519YS,Allison,08-01-2017,10-01-2017,Fedx,13-01-2017,yes,15-01-2017,Damaged Item
public class FraudDetection_Writable implements Writable {

	private String customerName;
	private String deliveredDate;
	private boolean returned;
	private String returnedDate;

	public FraudDetection_Writable() {
		set("", "", "no", "");
	}

	public void set(String name, String deliveredDate, String returned, String returnedDate) {
		this.customerName = name;
		this.deliveredDate = deliveredDate;
		if (returned.equalsIgnoreCase("yes"))
			this.returned = true;
		else
			this.returned = false;
		this.returnedDate = returnedDate;
	}

	public String getCustomerName() {
		return customerName;
	}

	public String getdeliveredDate() {
		return deliveredDate;
	}

	public boolean getReturned() {
		return returned;
	}

	public String getReturnedDate() {
		return returnedDate;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, customerName);
		WritableUtils.writeString(out, deliveredDate);
		out.writeBoolean(returned);
		WritableUtils.writeString(out, returnedDate);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.customerName = WritableUtils.readString(in);
		this.deliveredDate = WritableUtils.readString(in);
		this.returned = in.readBoolean();
		this.returnedDate = WritableUtils.readString(in);
	}

}
