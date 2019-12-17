package org.vertexium;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateOnly implements Serializable {
    static final long serialVersionUID = 42L;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final LocalDate date;

    public DateOnly(Date date) {
        this.date = date.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
    }

    public DateOnly(int year, int month, int day) {
        this.date = LocalDate.of(year, month, day);
    }

    @Override
    public String toString() {
        return this.date.format(DATE_FORMAT);
    }

    public ZonedDateTime getUtcDate() {
        return this.date.atStartOfDay(ZoneOffset.UTC);
    }

    public Date getDate() {
        return Date.from(this.date.atStartOfDay(ZoneOffset.UTC).toInstant());
    }
}
