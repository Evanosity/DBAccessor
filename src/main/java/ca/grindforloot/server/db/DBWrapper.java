package ca.grindforloot.server.db;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

/**
 * Base class for services operating on a MongoDB database
 * This is abstract and provides low level access to mongodb.
 */
public abstract class DBWrapper {
    private final MongoClient client;
    protected final MongoDatabase db;
    protected final ClientSession session;

    public DBWrapper(MongoClient client) {
        this.client = client;
        session = client.startSession();
        db = client.getDatabase(getDBName());
    }

    protected abstract String getDBName();

    protected void finalize(){
        session.close();
        client.close();
    }

    /**
     * Perform an action inside of a mongodb transaction.
     * This isn't ThreadSafe
     *
     * @param action - what is being run inside the script context
     * @return
     */
    public boolean doTransaction(Runnable action) {

        try{
            action.run();
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }

        /*
        //TODO consider transactionOptions
        return session.withTransaction(() -> {
            try {
                action.run();
                return true;
            }
            //if any error occurs, we simply report back. Might be worth throwing E instead? TODO
            catch(Exception e) {
                return false;
            }
        });*/
    }
}
