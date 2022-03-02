package ca.grindforloot.server.db;

import ca.grindforloot.server.Pair;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Static methods for composing Bson
 */
public class BsonService {

    public enum FilterOperator {
        EQUAL, NOT_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL
    }

    /**
     * Generates a Bson filter for a singular ID.
     * @param id
     * @return
     */
    protected static Bson getFilterForId(String id) {
        return generateFilter("_id", FilterOperator.EQUAL, new ObjectId(id));
    }

    /**
     * Generate a bson projection
     * @param projections - a set of strings; the property names we are projecting
     * @return the composed bson projections
     */
    protected static Bson generateProjections(Set<String> projections) {
        return Projections.include(projections.toArray(new String[projections.size()]));
    }

    /**
     * Generate a bson update
     * @param updates - a string-object map of the updates to apply
     * @return the composed bson updates
     */
    protected static Bson generateUpdates(Map<String, Object> updates) {
        List<Bson> bsonUpdates = new ArrayList<>();

        for(Map.Entry<String, Object> entry : updates.entrySet()) {

            //ensure that we parse keys into documents
            Object value = parseValue(entry.getValue());

            bsonUpdates.add(Updates.set(entry.getKey(), value));
        }


        return Updates.combine(bsonUpdates);
    }

    /**
     * Generate a composite bson filter
     * TODO helper methods for queries
     * @param filters - a string-<filteroperator-object> map of the filters. see Query and QueryService for impl
     * @return the composed Bson
     */
    protected static Bson generateCompositeFilter(Map<String, Pair<FilterOperator, Object>> filters) {

        List<Bson> builtFilters = new ArrayList<>();

        for(Map.Entry<String, Pair<FilterOperator, Object>> entry : filters.entrySet()) {
            String type = entry.getKey();
            Pair<FilterOperator, Object> rawFilter = entry.getValue();

            //Ensure that we parse keys into documents
            Object value = parseValue(rawFilter.getValue());

            builtFilters.add(generateFilter(type, rawFilter.getKey(), value));
        }

        //Compose the final filter.
        return Filters.and(builtFilters.toArray(new Bson[builtFilters.size()]));
    }

    /**
     * Generate a SINGLE bson filter
     * @param fieldName - the field we're operating for
     * @param op - the FilterOperator we're using(Equal, Not equal, etc)
     * @param value - the value we're comparing against
     * @return the bson filter
     */
    protected static Bson generateFilter(String fieldName, FilterOperator op, Object value) {
        switch(op) {
            case EQUAL:
                return Filters.eq(fieldName, value);
            case NOT_EQUAL:
                return Filters.ne(fieldName, value);
            case GREATER:
                return Filters.gt(fieldName, value);
            case GREATER_EQUAL:
                return Filters.gte(fieldName, value);
            case LESS:
                return Filters.lt(fieldName, value);
            case LESS_EQUAL:
                return Filters.lte(fieldName, value);
            default:
                throw new IllegalArgumentException("Invalid filter operator " + op.toString());
        }
    }

    /**
     * Convert raw objects into documents
     * Notably, {@link Key} -> {@link Document}
     * This is its own method because in the case of lists, it calls itself recursively.
     * @param obj the raw object
     * @return the formatted object
     */
    protected static Object parseValue(Object obj) {
        //if this object is a key, we convert it to a document.
        if(obj instanceof Key) {
            Key key = (Key) obj;
            return key.toDocument();
        }
        //if this object is a list, we iterate over it, parsing each time
        if(obj instanceof List){
            List<Object> list = new ArrayList<>();

            for(Object o : (List<?>)obj)
                list.add(parseValue(o));

            return list;
        }

        //otherwise, we simply return the object untouched.
        return obj;
    }
}