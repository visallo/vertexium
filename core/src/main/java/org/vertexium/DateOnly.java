package org.vertexium;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateOnly implements Serializable {
    static final long serialVersionUID = 42L;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final Date date;

    public DateOnly(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DATE);
        this.date = new GregorianCalendar(year, month, day).getTime();
    }

    public DateOnly(int year, int month, int day) {
        this.date = new GregorianCalendar(year, month, day).getTime();
    }

    @Override
    public String toString() {
        return DATE_FORMAT.format(this.date);
    }

    public Date getDate() {
        return this.date;
    }
}
