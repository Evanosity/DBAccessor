package ca.elixa.db;

import java.util.Set;

import ca.elixa.classpool.ClassPool;
import org.bson.Document;

/**
 * This class is for generating subclasses of {@link Entity}. Implementations of {@link DBService} are responsible
 * for their generation of subclasses.
 * 
 * @author Evan
 *
 */
public class EntityFactory {

	private final ClassPool<? extends Entity> pool;

	public EntityFactory(String path){
		pool = new ClassPool<>(path, Entity.class);
	}
	
	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc) {
		return createEntityObject(db, type, doc, false, null);
	}

	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc, boolean isNew){
		return createEntityObject(db, type, doc, true, null);
	}
	
	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc, Set<String> projections) {
		return createEntityObject(db, type, doc, false, projections);
	}

	/**
	 * @param isNew - if this is a fresh entity
	 * @param projections - any projections on this entity.
	 * @param <T extends Entity> - the object type we're creating.
	 * @return the entity.
	 */
	protected <T extends Entity> T createEntityObject(final DBService db, final String type, Document doc, Boolean isNew, Set<String> projections){
		T raw = (T) pool.get(type);
		T result = raw.instantiate();
		result.init(db, doc, isNew, projections);
		return result;
	}
}
