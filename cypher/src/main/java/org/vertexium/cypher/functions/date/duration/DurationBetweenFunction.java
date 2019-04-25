package org.vertexium.cypher.functions.date.duration;

import org.vertexium.cypher.CypherDuration;
import org.vertexium.cypher.CypherZonedTime;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class DurationBetweenFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 2);
        Object arg0 = arguments[0];
        Object arg1 = arguments[1];

        if (arg0 == null || arg1 == null) {
            return null;
        }

        if (isLocalTimeOrCypherZonedTime(arg0) && isLocalTimeOrCypherZonedTime(arg1)) {
            ZonedDateTime startOfDay = startOfDay(ZonedDateTime.now());
            arg0 = toDateTime(startOfDay, arg0);
            arg1 = toDateTime(startOfDay, arg1);
        } else if (isLocalTimeOrCypherZonedTime(arg0)) {
            arg0 = toDateTime(startOfDay(arg1), arg0);
        } else if (isLocalTimeOrCypherZonedTime(arg1)) {
            arg1 = toDateTime(startOfDay(arg0), arg1);
        }

        LocalDate arg0Date = LocalDate.from((TemporalAccessor) arg0);
        LocalDate arg1Date = LocalDate.from((TemporalAccessor) arg1);

        Period periodBetween = Period.between(arg0Date, arg1Date);
        Duration duration = Duration.between((Temporal) arg0, (Temporal) arg1);

        return new CypherDuration(periodBetween, duration);
    }

    private Temporal toDateTime(ZonedDateTime startOfDay, Object t) {
        if (t instanceof LocalTime) {
            LocalTime time = (LocalTime) t;
            long nanos = (((((time.getHour() * 60) + time.getMinute()) * 60) + time.getSecond()) * 1000L * 1000L * 1000L) + time.getNano();
            return startOfDay.plus(nanos, ChronoUnit.NANOS);
        }

        if (t instanceof CypherZonedTime) {
            CypherZonedTime time = (CypherZonedTime) t;
            return time.toZonedDateTime(startOfDay);
        }

        throw new VertexiumCypherNotImplemented("unhandled: " + t.getClass().getName());
    }

    private boolean isLocalTimeOrCypherZonedTime(Object o) {
        return o instanceof LocalTime || o instanceof CypherZonedTime;
    }

    private ZonedDateTime startOfDay(Object temporal) {
        if (temporal instanceof ZonedDateTime) {
            return ((ZonedDateTime) temporal).truncatedTo(ChronoUnit.DAYS);
        } else {
            throw new VertexiumCypherNotImplemented("unhandled: " + temporal.getClass().getName());
        }
    }
}
