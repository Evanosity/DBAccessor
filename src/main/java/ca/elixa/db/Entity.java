package ca.elixa.db;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import io.vertx.core.json.JsonObject;

/**
 * Abstract class, the root of the entity system. Wraps a MongoDB Document with helper methods
 * 
 * All subclasses should implement every constructor.
 * 
 * You do NOT need to provide a key object to this entity; it gets generated from the document.
 * 
 * @author Evan
 *
 */
public abstract class Entity implements Cloneable {
	protected DBService db;
	protected Document raw;
	private Key key;
	private Set<String> projections; //This can be null
	
	private Boolean isNew;

	protected Entity(){};
	
	/**
	 * Internal constructor
	 * @param db
	 * @param raw
	 * @param isNew
	 * @param projections
	 */
	protected void init(DBService db, Document raw, boolean isNew, Set<String> projections) {
		this.db = db;
		this.raw = raw;
		this.isNew = isNew;
		
		this.projections = projections;

		//if the entity is phresh, we need to generate a key for it
		if(isNew) {
			this.key = db.generateKey(getType());
			raw.put("_id", new ObjectId(key.getId()));
		}
		//otherwise, create the key out of the type and ID.
		else {
			this.key = new Key(getType(), raw.getObjectId("_id").toHexString());
		}
	}
	
	/**
	 * TODO consider reflectively grabbing this?
	 */
	public abstract String getType();

	protected abstract <T extends Entity> T instantiate();
	
	public boolean isNew() {
		return isNew;
	}
	
	public boolean projected() {
		return projections != null && false == projections.isEmpty();
	}
	
	/**
	 * This will return null if no projections were set. or at least it should....
	 * @return
	 */
	public Set<String> getProjections(){
		return projections;
	}
	
	public String getName() {
		return raw.getString("name");
	}

	/**
	 * Given a List of Keys on an entity, fetch those entities.
	 * @param key
	 * @return
	 * @param <T>
	 */
	public <T extends Entity> List<T> getReferencedEntityList(String key){
		List<Key> keys = getKeyList(key);

		return db.getEntities(keys);
	}

	/**
	 * Get a list of keys. Can be empty.
	 * @param key
	 * @return
	 */
	public List<Key> getKeyList(String key){
		List<Document> documents = getListValue(key, Document.class);

		if(documents == null || documents.size() == 0){
			return new ArrayList<>();
		}

		List<Key> results = new ArrayList<>();

		for(Document doc : documents)
			results.add(db.getKeyFromDoc(doc));

		return results;
	}

	public void addKeyToList(String targetKey, Entity toAdd){
		addKeyToList(targetKey, toAdd.getKey());
	}

	public void addKeyToList(String targetKey, Key toAdd){
		List<Key> current = getKeyList(targetKey);
		current.add(toAdd);

		setValue(targetKey, current);
	}

	/**
	 * Get a list of T off of the entity
	 * @param key
	 * @param type
	 * @return
	 * @param <T>
	 */
	protected <T> List<T> getListValue(String key, Class<T> type){
		return raw.getList(key, type);
	}
	
	public Object getValue(String key) {
		return raw.get(key);
	}
	public String getStringValue(String key){
		return (String) getValue(key);
	}
	protected Date getDateValue(String key){
		return (Date) getValue(key);
	}

	protected Long getLongValue(String key){
		return raw.getLong(key);
	}

	protected Double getDoubleValue(String key){
		return raw.getDouble(key);
	}

	/**
	 *
	 * @param key
	 * @return an empty array if the value is null
	 */
	protected byte[] getBinaryValue(String key) {
		Object value = getValue(key);
		if(value == null)
			return new byte[0];
		return ((Binary) value).getData();
	}
	protected ObjectId getObjectIdValue(String key){
		return (ObjectId) getValue(key);
	}
	protected <T extends Entity> T getEntityFromKeyValue(String key){
		Key k = getKeyValue(key);

		if(k == null)
			return null;

		return db.getEntity(k);
	}
	
	public void setValue(String key, Object value) {
		raw.put(key, BsonService.parseValue(value));
	}
	
	/**
	 * Embeds a key onto an entity
	 * @param key
	 */
	protected void setKeyValue(String key, Key value) {
		setValue(key, value);
	}
	protected void setBinaryValue(String key, byte[] value){
		setValue(key, new Binary(value));
	}
	
	protected Key getKeyValue(String property) {
		Document rawKey = (Document) getValue(property);

		if(rawKey == null)
			return null;
		
		return new Key(rawKey);
	}
	
	/**
	 * TODO once the schema is setup, split this into two methods.
	 * One that reads if the entity's schema has a certain field
	 * One that reads if this entity has a non-null value for a field
	 * @param key
	 * @return
	 */
	public boolean hasValue(String key) {
		return raw.containsKey(key);
	}
	
	public Key getKey() {
		return key;
	}

	public String getKeyString(){
		return getKey().toString();
	}
	
	public String getId() {
		return getKey().getId();
	}

	/**
	 * Do not allow direct access to the raw underlying document.
	 *
	 * INSTEAD, allow them to iterate over an unmodifiable version of the document
	 */
	public void iterateOverAllProperties(Consumer<? super Entry<String, Object>> transformation){
		Collections.unmodifiableCollection(raw.entrySet()).forEach(transformation);
	}

	/**
	 * Creates a perfect copy of this entity, with a unique key.
	 * @return the cloned entity
	 */
	@Override
	public Entity clone(){
		Entity result = db.createEntity(getType());

		for(Entry<String, Object> entry : raw.entrySet()) {
			if(entry.getKey().equals("_id")) continue; //dont copy the ID
			result.setValue(entry.getKey(), entry.getValue());
		}

		return result;
	}

	
	/**
	 * Create a Vert.x JsonObject representation of this entity
	 * @return
	 */
	public JsonObject toJson() {
		JsonObject result = new JsonObject();
		
		for(Entry<String, Object> entry : raw.entrySet())
			result.put(entry.getKey(), entry.getValue());
		
		return result;
	}
}
