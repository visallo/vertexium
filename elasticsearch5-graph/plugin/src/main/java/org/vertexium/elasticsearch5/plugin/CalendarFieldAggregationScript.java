package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

public class CalendarFieldAggregationScript extends VertexiumSearchScript {
    private final int calendarField;
    private final ZoneId zoneId;
    private final String fieldName;

    public CalendarFieldAggregationScript(SearchLookup lookup, Map<String, Object> vars) {
        this(
            lookup,
            (int) (Integer) vars.get("calendarField"),
            ZoneId.of((String) vars.get("tzId")),
            (String) vars.get("fieldName")
        );
    }

    public CalendarFieldAggregationScript(SearchLookup lookup, int calendarField, ZoneId zoneId, String fieldName) {
        super(lookup);
        this.calendarField = calendarField;
        this.zoneId = zoneId;
        this.fieldName = fieldName;
    }

    @Override
    protected Object run(LeafSearchLookup leafSearchLookup) {
        LeafDocLookup doc = leafSearchLookup.doc();
        if (!doc.containsKey(fieldName)) {
            return -1;
        }
        List<?> values = doc.get(fieldName).getValues();
        for (Object value : values) {
            ZonedDateTime d;
            if (value instanceof Long) {
                d = Instant.ofEpochMilli((Long) value).atZone(zoneId);
            } else {
                throw new VertexiumElasticsearchPluginException("Unhandled value type: " + value.getClass().getName());
            }
            switch (calendarField) {
                case Calendar.DAY_OF_MONTH:
                    return d.getDayOfMonth();
                case Calendar.DAY_OF_WEEK:
                    int i = d.getDayOfWeek().getValue() + 1;
                    return i > 7 ? i - 7 : i;
                case Calendar.HOUR_OF_DAY:
                    return d.getHour();
                case Calendar.MONTH:
                    return d.getMonthValue() - 1;
                case Calendar.YEAR:
                    return d.getYear();
                default:
                    return GregorianCalendar.from(d).get(calendarField);
            }
        }
        return -1;
    }
}
