package com.doit.chap01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator{
	
    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateTemperaturePair pari = (DateTemperaturePair) a;
		DateTemperaturePair pari2 = (DateTemperaturePair) b;
		return pari.getYearMonth().compareTo(pari2.getYearMonth());
	}
	
	

}
