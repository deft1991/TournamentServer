package session_tools;

import data_object.IDataObject;
import data_object.Table;
import data_object.Variable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class Session {

    public final static String VARIALBE_TABLE_NAME = "variables";

    private long sessionId;
    private Connection sqlConnection;
    private Set<String> sessionTables = new HashSet<>();

    public Session(long sessionId) {
        this.sessionId = sessionId;
        try {
            sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/javastudy","root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Set<String> getSessionTables() {
        return sessionTables;
    }

    public void dropTableSession() throws SQLException {
        for (String oneTableName : sessionTables) {
            Table.dropTable(getSqlConnection(), oneTableName);
        }
    }

    public long getSessionId() {
        return sessionId;
    }

    public Connection getSqlConnection() {
        return sqlConnection;
    }

    public String getResultByAction(String actionName, String inputData) throws SQLException {
        switch (actionName) {
            case "test" : return getActionHello();
            case "calculate" : return getActionHello();
            case "get_table" : return getActionHello();
            case "save_table" : return getActionHello();
            case "start_service" : return getActiongStartService(inputData);
            case "close_session" :
                SessionController.getInstance().closeSession(sessionId);
                return "";
            default: return "hello";
        }
    }

    private String getActionCalulate(String inputData) {
        return "";
    }

    private String getActionHello() {
        return "hello";
    }

    private String getActiongStartService(String inputData) throws SQLException {
        JSONArray dataObjList = new JSONArray(inputData);
        for (int i = 0; i < dataObjList.length(); i++) {
            JSONObject oneDataObj = dataObjList.getJSONObject(i);
            if ("table".equals(oneDataObj.getString("datatype"))) {
                Table oneTable = new Table(oneDataObj, sessionId);
                sessionTables.add(oneTable.getName());
                oneTable.insertInDB(getSqlConnection());
            }
        }
        return String.valueOf(sessionId);
    }



    public IDataObject getDataObject(String name) throws SQLException {
        String objectName = name + "_" + getSessionId();
        if (getSessionTables().contains(objectName)) {
            try (PreparedStatement ps = getSqlConnection().prepareStatement(
                    "selct * from " + objectName
            )) {
                return new Table(ps.executeQuery());
            }
        } else {
            try (PreparedStatement ps = getSqlConnection().prepareStatement(
                    "select * from " + VARIALBE_TABLE_NAME + " where variable_name = ?"
            )) {
                ps.setString(1, name);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return new Variable(rs);
                }
            }
        }
        throw new RuntimeException("Ошибка! Не верно указан оператор");
    }

}
