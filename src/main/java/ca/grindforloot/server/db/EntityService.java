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
	
	protected <T extends Entity> T buildEntity(final String type, Document doc) {
		return createEntityObject(type, doc, false, null);
	}

	protected <T extends Entity> T buildEntity(final String type, Document doc, boolean isNew){
		return createEntityObject(type, doc, true, null);
	}
	
	protected <T extends Entity> T buildEntity(final String type, Document doc, Set<String> projections) {
		return createEntityObject(type, doc, false, projections);
	}

	/**
	 *
	 * @param key - this is necessary to
	 * @param isNew - if this is a fresh entity
	 * @param projections - any projections on this entity.
	 * @param <T extends Entity> - the object type we're creating.
	 * @return the entity.
	 */
	protected abstract <T extends Entity> T createEntityObject(final String type, Document doc, Boolean isNew, Set<String> projections);
}
