package ca.grindforloot.server.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;


/**
 * The main point of access to MongoDB
 * My goal is to not expose the MongoDB api outside of this package.
 * 
 * See {@link EntityService} for the generation of entity objects.
 * 
 * @author Evan
 *
 * TODO consider caching the collections we've called?
 *
 */
public class DBService extends DBWrapper{
	private final EntityService entityService;

	public DBService(MongoClient client, EntityService cs) {
		super(client);
		
		entityService = cs;
	}

	@Override
	//TODO this is probably an environment variable
	protected String getDBName() {
		return "gfl-test";
	}

	public Long test(){
		Bson filter = BsonService.getFilterForId("621b087015fbea9eed172e7c");

		Document result = fetchRawInternal("test", filter, new Document()).get(0);

		return result.getLong("Visits");
	}

	public void testIncrement(){

		Boolean test = doTransaction(() -> {
			Bson filter = BsonService.getFilterForId("621b087015fbea9eed172e7c");

			Document result = fetchRawInternal("test", filter, new Document()).get(0);

			Long value = result.getLong("Visits");
			value = value+1;
			result.put("Visits", value);
			result.put("NewValue", value);

			System.out.println(value);

			MongoCollection<Document> col = db.getCollection("test");
			col.replaceOne(filter, result);

		});

		if(!test) System.out.println("a whoopsie has happened");
	}

	public Key getKey(String type, String id) {
		return new Key(type, id);
	}

	public <T extends Entity> T createEntity(String type){
		return entityService.buildEntity(new Document());
	}

	public Key generateKey(String type) {
		
		boolean resolved = false;
		
		ObjectId random = null;
		
		while(!resolved) {
			random = new ObjectId();
			
			Key key = new Key(type, random.toHexString());

			if(entityExists(key))
				resolved = true;
		}
		
		return new Key(type, random.toHexString());
	}
	

	
	/**
	 * Inserts a single entity into the DB
	 * @param ent
	 */
	public void put(Entity ent) {
		MongoCollection<Document> col = db.getCollection(ent.getType());
		
		putInternal(ent, col);
	}
	
	/**
	 * Same as calling put(Iterable<Entity>)
	 * @param entities
	 */
	public void put(Entity...entities ) {
		put(Arrays.asList(entities));
	}
	
	/**
	 * Insert a list of entities into the database
	 * @param ents
	 */
	public void put(Iterable<Entity> ents) {
		Map<String, List<Entity>> sorted = sortEntitiesByType(ents);
		
		for(Entry<String, List<Entity>> entry : sorted.entrySet()) {
			MongoCollection<Document> col = db.getCollection(entry.getKey());
						
			for(Entity ent : entry.getValue())
				putInternal(ent, col);
		}
	}
	
	/**
	 * Put an entity to the DB.
	 * @param ent - the Entity to put
	 * @param col - the MongoCollection we're putting this document to
	 */
	private void putInternal(Entity ent, MongoCollection<Document> col) {
		
		//TODO consider a put event handler
		
		if(ent.projected()) {
			//TODO log that we can't save projected entities.
			return;
		}
		
		if(ent.isNew())
			col.insertOne(session, ent.raw);
		else
			col.replaceOne(session, BsonService.getFilterForId(ent.getId()), ent.raw);
	}
	
	public void deleteEntity(Entity ent) {		
		delete(ent.getKey());
	}
	
	/**
	 * Delete entities from the DB. This is the same as deleteEntity(Iterable)
	 * @param entities
	 */
	public void deleteEntity(Entity...entities) {
		deleteEntity(Arrays.asList(entities));
	}
	
	/**
	 * Delete entities from the DB.
	 * @param entities
	 */
	public void deleteEntity(Iterable<Entity> entities) {
		List<Key> keys = getKeysFromEntities(entities);
		
		delete(keys);
	}
	
	/**
	 * Delete an entity from the DB based on its key
	 * @param key
	 */
	public void delete(Key key) {
		MongoCollection<Document> col = db.getCollection(key.getType());
		
		deleteInternal(key, col);
	}
	/**
	 * Delete a collection of entities by their key. This is the same as delete(Iterable)
	 * @param keys
	 */
	public void delete(Key...keys) {
		delete(Arrays.asList(keys));
	}
	/**
	 * Delete a collection of entities by their key.
	 * @param keys
	 */
	public void delete(Iterable<Key> keys) {
		
		Map<String, List<Key>> sorted = sortKeysByType(keys);
		
		for(Entry<String, List<Key>> entry : sorted.entrySet()) {
			MongoCollection<Document> col = db.getCollection(entry.getKey());
			
			for(Key key : entry.getValue())
				deleteInternal(key, col);
		}
	}
	
	/**
	 * Delete a key from the DB
	 * @param key
	 * @param col
	 */
	private void deleteInternal(Key key, MongoCollection<Document> col) {
		col.deleteOne(session, BsonService.getFilterForId(key.getId()));
	}
	
	/**
	 * Fetch a list of entities from a list of keys.
	 * 
	 * All of these entities will not *necessarily* be the same type. You will need to cast to the appropriate type.
	 * @param keys
	 * @return
	 */
	public List<Entity> getEntities(Iterable<Key> keys){
		List<Entity> result = new ArrayList<>();
		
		Map<String, List<Key>> sorted = sortKeysByType(keys);
		
		for(Entry<String, List<Key>> entry : sorted.entrySet()) {
			String type = entry.getKey();
			List<ObjectId> ids = new ArrayList<>();

			for(Key key : entry.getValue()) 
				ids.add(new ObjectId(key.getId()));
			
			Bson filter = Filters.in("_id", ids);
			
			MongoCollection<Document> col = db.getCollection(type);
			
			for(Document doc : col.find(session, filter)) 
				result.add(entityService.buildEntity(doc));
		}
		
		return result;
	}
	
	/**
	 * Fetch a single entity from a key.
	 * @param key
	 * @return
	 */
	public <T extends Entity> T getEntity(Key key) {
		
		List<Document> docs = fetchRawInternal(key.getType(), BsonService.getFilterForId(key.getId()), null);
		
		if(docs.size() != 1)
			throw new IllegalStateException("cant have multiple docs with the same identifier. delete this project.");
		
		return entityService.buildEntity(docs.get(0));
	}

	/**
	 * @param key
	 * @return true if an entity already exists for the given key.
	 */
	public boolean entityExists(Key key){
		MongoCollection<Document> collection = db.getCollection(key.getType());
		return collection.countDocuments(session, BsonService.getFilterForId(key.getId())) == 0;
	}
	
	/**
	 * returns a list of built entities, given a type and a bson filter.
	 * @param type
	 * @param filter
	 * @return
	 */
	protected <T extends Entity> List<T> fetchInternal(String type, Bson filter){
		return fetchInternal(type, filter, null);
	}
	
	/**
	 * Returns a list of built entities, given a type and a Bson Filter. Any group fetch runs through this method.
	 * We create the entities using this.
	 * @param type - the entity type we're fetching
	 * @param filter - the composed bson filter
	 * @param projections - a set of the fields we're projection. This can be null.
	 * @return
	 */
	protected <T extends Entity> List<T> fetchInternal(String type, Bson filter, Set<String> projections){
		
		Bson composedProj = BsonService.generateProjections(projections);
		
		List<Document> rawList = fetchRawInternal(type, filter, composedProj);
		List<T> result = new ArrayList<>();
		
		for(Document doc : rawList) {
			Key key = new Key(type, doc.getObjectId("_id"));
						
			result.add(entityService.buildEntity(doc));
		}
		
		return result;
	}

	public <T extends Entity> List<T> runEntityQuery(Query q){
		Bson filter = BsonService.generateCompositeFilter(q.filters);

		return fetchInternal(q.getType(), filter, q.projections);
	}
	
	/**
	 * Fetches a raw list of documents from the given collection.
	 * @param collection - the type of entity
	 * @param filter - the composed Bson filters
	 * @param projections - the composed Bson projections
	 * @return
	 */
	private List<Document> fetchRawInternal(String collection, Bson filter, Bson projections) {
		List<Document> result = new ArrayList<>();

		FindIterable<Document> dbResult = db.getCollection(collection).find(filter).projection(projections);

		for (Document doc : dbResult)
			result.add(doc);

		return result;
	}

	/**
	 * queries
	 */
	public void runDeleteQuery(Query q) {
		Bson filter = BsonService.generateCompositeFilter(q.filters);

		db.getCollection(q.getType()).deleteMany(session, filter);

	}
	public void runUpdate(Query q) {
		Bson filters = BsonService.generateCompositeFilter(q.filters);
		Bson updates = BsonService.generateUpdates(q.updates);

		db.getCollection(q.getType()).updateMany(session, filters, updates);
	}
	public Long runCount(Query q) {
		Bson filters = BsonService.generateCompositeFilter(q.filters);

		return db.getCollection(q.getType()).countDocuments(session, filters);
	}


	
	/**
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	Below this point are helper methods for sorting entities and extracting information from a collection of entities/keys
	 */
	
	/**
	 * Extracts a list of keys from a list of entities
	 * @param entities
	 * @return
	 */
	public List<Key> getKeysFromEntities(Iterable<Entity> entities){
		List<Key> result = new ArrayList<>();
		
		for(Entity ent : entities)
			result.add(ent.getKey());
		
		return result;
	}
	
	/**
	 * Extracts a list of documents from a list of entities
	 * @param entities
	 * @return
	 */
	protected List<Document> getRawEntities(Iterable<Entity> entities){
		List<Document> result = new ArrayList<>();
		
		for(Entity ent : entities) 
			result.add(ent.raw);
		
		return result;
	}
	
	/**
	 * Sorts a list of entities into a map of type-list<entity>
	 * @param ents
	 * @return
	 */
	private Map<String, List<Entity>> sortEntitiesByType(Iterable<Entity> ents){
		Map<String, List<Entity>> result = new HashMap<>();
		
		for(Entity ent : ents) {
			String type = ent.getType();
			
			List<Entity> current = result.get(type);
			if(current == null) current = new ArrayList<>();
			
			current.add(ent);
			
			result.put(type, current);
		}
		
		return result;
	}
	
	/**
	 * Sorts a list of keys into a map of type-list<key>
	 * @param keys
	 * @return
	 */
	private Map<String, List<Key>> sortKeysByType(Iterable<Key> keys){
		Map<String, List<Key>> result = new HashMap<>();
		
		for(Key key : keys) {
			String type = key.getType();
			
			List<Key> current = result.get(type);
			if(current == null) current = new ArrayList<>();
			
			current.add(key);
			
			result.put(type, current);
		}
		
		return result;
	}
}
