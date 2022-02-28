package ca.grindforloot.server.db;

import java.util.Set;
import org.bson.Document;

/**
 * This class is for generating subclasses of {@link Entity}. Implementations of {@link DBService} are responsible
 * for their generation of subclasses.
 * 
 * @author Evan
 *
 */
public abstract class EntityService {
	
	public DBService db;
	
	public EntityService(DBService db) {
		this.db = db;
	}
	
	/**
	 * Create a new entity
	 * @param <T> the type of object we're creating
	 * @param type the string of the object we're creating If T and Type don't match, that's cringe.
	 * @return
	 */
	public <T extends Entity> T createEntity(String type) {
		Key key = db.generateKey(type);
		
		return createEntityObject(key, new Document(), true, null);
	}
	
	public <T extends Entity> T buildEntity(Key key, Document doc) {
		return createEntityObject(key, doc, false, null);
	}
	
	public <T extends Entity> T buildEntity(Key key, Document doc, Set<String> projections) {
		return createEntityObject(key, doc, false, projections);
	}

	/**
	 *
	 * @param key - this is necessary to
	 * @param isNew - if this is a fresh entity
	 * @param projections - any projections on this entity.
	 * @param <T extends Entity> - the object type we're creating.
	 * @return the entity.
	 */
	protected abstract <T extends Entity> T createEntityObject(Key key, Document doc, Boolean isNew, Set<String> projections);
}
