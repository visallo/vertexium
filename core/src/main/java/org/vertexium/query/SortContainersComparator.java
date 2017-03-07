package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.VertexiumObject;
import org.vertexium.VertexiumException;

import java.util.Comparator;
import java.util.List;

import static org.vertexium.util.IterableUtils.toList;

public class SortContainersComparator<T extends VertexiumObject> implements Comparator<T> {
    private final List<QueryBase.SortContainer> sortContainers;

    public SortContainersComparator(List<QueryBase.SortContainer> sortContainers) {
        this.sortContainers = sortContainers;
    }

    @Override
    public int compare(T elem1, T elem2) {
        for (QueryBase.SortContainer sortContainer : sortContainers) {
            int result = compare(sortContainer, elem1, elem2);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compare(QueryBase.SortContainer sortContainer, T vertexiumObject1, T vertexiumObject2) {
        if(vertexiumObject1 instanceof Element && vertexiumObject2 instanceof Element) {
            Element elem1 = (Element) vertexiumObject1;
            Element elem2 = (Element) vertexiumObject2;
            List<Object> elem1PropertyValues = toList(elem1.getPropertyValues(sortContainer.propertyName));
            List<Object> elem2PropertyValues = toList(elem2.getPropertyValues(sortContainer.propertyName));
            if (elem1PropertyValues.size() > 0 && elem2PropertyValues.size() == 0) {
                return -1;
            } else if (elem2PropertyValues.size() > 0 && elem1PropertyValues.size() == 0) {
                return 1;
            } else {
                for (Object elem1PropertyValue : elem1PropertyValues) {
                    for (Object elem2PropertyValue : elem2PropertyValues) {
                        int result = comparePropertyValues(elem1PropertyValue, elem2PropertyValue);
                        if (result != 0) {
                            return sortContainer.direction == SortDirection.ASCENDING ? result : -result;
                        }
                    }
                }
            }
            return 0;
        }else{
            throw new VertexiumException("unexpected searchable item combination: "+vertexiumObject1.getClass().getName()+", "+vertexiumObject2.getClass().getName());
        }
    }

    @SuppressWarnings("unchecked")
    private int comparePropertyValues(Object v1, Object v2) {
        if (v1.getClass() == v2.getClass() && v1 instanceof Comparable) {
            return ((Comparable) v1).compareTo(v2);
        }
        return 0;
    }
}
