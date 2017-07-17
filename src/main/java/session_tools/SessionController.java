package session_tools;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class SessionController {

    private final Map<Long, Session> sessionMap = new HashMap<Long, Session>();
    private static SessionController instance;
    private long maxSessionId;

    public static synchronized SessionController getInstance() {
        if (instance == null) {
            instance = new SessionController();
        }
        return instance;
    }

    private SessionController() {}

    public long generateSessionId() {
        return ++maxSessionId;
    }

    public Session getSessionById(long sessionId) {
        Session sessionInstance = sessionMap.get(sessionId);
        if (sessionInstance == null) {
            sessionMap.put(sessionId, sessionInstance = new Session(sessionId));
        }
        return sessionInstance;
    }

    public void closeSession(long sessionId) throws SQLException {
        Session sessionInstance = sessionMap.get(sessionId);
        if (sessionInstance != null) {
            sessionInstance.dropTableSession();
            sessionMap.remove(sessionId);
            sessionInstance.getSqlConnection().close();
        }
    }
}
