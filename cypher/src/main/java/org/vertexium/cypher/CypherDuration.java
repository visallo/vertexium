package org.vertexium.cypher;

import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class CypherDuration {
    private static final long NANOS_IN_SECOND = 1000L * 1000L * 1000L;
    private static final long NANOS_IN_DAY = 24L * 60L * 60L * NANOS_IN_SECOND;
    private final Period period;
    private final long nanos;
    private final boolean negative;

    public CypherDuration(Period period, Duration duration) {
        this.negative = (period.isNegative() || duration.isNegative());

        if (period.isNegative()) {
            period = period.negated();
        }
        if (duration.isNegative()) {
            duration = duration.negated();
        }

        this.nanos = ((duration.getSeconds() * NANOS_IN_SECOND) + duration.getNano()) % NANOS_IN_DAY;
        this.period = period.minusDays(this.nanos > 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        long nanos = this.nanos;

        if (nanos == 0 && period.isZero()) {
            return "PT0S";
        }

        long years = period.getYears();
        if (years > 0) {
            if (negative) {
                result.append("-");
            }
            result.append(years).append("Y");
        }

        long months = period.getMonths();
        if (months > 0) {
            if (negative) {
                result.append("-");
            }
            result.append(months).append("M");
        }

        long days = period.getDays();
        if (days > 0) {
            if (negative) {
                result.append("-");
            }
            result.append(days).append("D");
        }

        if (nanos > 0) {
            result.append('T');
        }

        long hours = TimeUnit.NANOSECONDS.toHours(nanos);
        if (hours > 0) {
            if (negative) {
                result.append("-");
            }
            result.append(hours).append("H");
            nanos -= TimeUnit.HOURS.toNanos(hours);
        }

        long minutes = TimeUnit.NANOSECONDS.toMinutes(nanos);
        if (minutes > 0) {
            if (negative) {
                result.append("-");
            }
            result.append(minutes).append("M");
            nanos -= TimeUnit.MINUTES.toNanos(minutes);
        }

        if (nanos > 0) {
            long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
            nanos -= TimeUnit.SECONDS.toNanos(seconds);
            if (negative) {
                result.append("-");
            }
            result.append(seconds);
            if (nanos > 0) {
                double n = ((double) nanos) / (double) NANOS_IN_SECOND;
                result.append(("" + n).substring(1));
            }
            result.append("S");
        }

        return "P" + result.toString();
    }

    public Object getProperty(String propertyName) {
        switch (propertyName) {
            case "days":
                return getDays();
            case "seconds":
                return getSeconds();
            case "nanosecondsOfSecond":
                return getNanosecondsOfSecond();
            default:
                throw new VertexiumCypherTypeErrorException("Cannot access property " + propertyName + " of a duration");
        }
    }

    private long getNanosecondsOfSecond() {
        long l = nanos % NANOS_IN_SECOND;
        if (negative && l > 0) {
            return NANOS_IN_SECOND - l;
        } else {
            return l;
        }
    }

    public long getDays() {
        return negate(TimeUnit.NANOSECONDS.toDays(nanos));
    }

    public long getSeconds() {
        return negate(TimeUnit.NANOSECONDS.toSeconds(nanos));
    }

    public long getMonths() {
        return negate(period.getMonths());
    }

    private long negate(long z) {
        if (negative) {
            return -z;
        }
        return z;
    }

    public CypherDuration truncatedTo(ChronoUnit units) {
        Period period = this.period;
        Duration duration = Duration.ofNanos(nanos);
        if (negative) {
            period = period.negated();
            duration = duration.negated();
        }
        switch (units) {
            case SECONDS:
                return new CypherDuration(Period.ZERO, duration);
            case DAYS:
                return new CypherDuration(period, Duration.ZERO);
            case MONTHS:
                return new CypherDuration(period.withDays(0), Duration.ZERO);
            default:
                throw new VertexiumCypherNotImplemented("Unimplemented truncate to: " + units);
        }
    }
}
