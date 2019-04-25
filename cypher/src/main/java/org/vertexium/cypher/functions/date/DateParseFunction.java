package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class DateParseFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 0, 1);
        if (arguments.length == 0) {
            return ctx.getNow();
        }

        if (arguments.length == 1) {
            Object arg0 = arguments[0];

            if (arg0 instanceof String) {
                return parseString(ctx, (String) arg0);
            }

            if (arg0 instanceof Map) {
                return fromMap(ctx, (Map) arg0);
            }

            throw new VertexiumCypherTypeErrorException(arg0, String.class, Map.class);
        }

        throw new VertexiumCypherException("Expected 0 or 1 arguments. Found " + arguments.length);
    }

    protected Object fromMap(VertexiumCypherQueryContext ctx, Map map) {
        Number year = (Number) map.get("year");
        Number month = (Number) map.get("month");
        Number day = (Number) map.get("day");
        Number hour = (Number) map.get("hour");
        Number minute = (Number) map.get("minute");
        Number second = (Number) map.get("second");
        String timeZone = (String) map.get("timezone");
        ZoneId zoneId;
        if (timeZone == null) {
            zoneId = ctx.getZoneId();
        } else {
            zoneId = ZoneId.of(timeZone);
        }
        ZonedDateTime now = ctx.getNow();
        int yearInt = year == null ? now.getYear() : year.intValue();
        int monthInt = month == null ? now.getMonthValue() : month.intValue();
        int dayInt = day == null ? now.getDayOfMonth() : day.intValue();
        int hourInt = hour == null ? 0 : hour.intValue();
        int minuteInt = minute == null ? 0 : minute.intValue();
        int secondInt = second == null ? 0 : second.intValue();
        return ZonedDateTime.of(
            yearInt,
            monthInt,
            dayInt,
            hourInt,
            minuteInt,
            secondInt,
            0,
            zoneId
        );
    }

    protected abstract Object parseString(VertexiumCypherQueryContext ctx, String arg0);
}
