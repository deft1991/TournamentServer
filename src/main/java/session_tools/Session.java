package session_tools;

import calculate.Formula;
import data_object.*;
import org.json.JSONArray;
import org.json.JSONObject;
import tools.MyTournamentException;

import java.sql.*;
import java.util.*;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class Session {

    public final static String VARIALBE_TABLE_NAME = "variables";

    private long sessionId;
    private Connection sqlConnection;
    private Set<String> sessionTables = new HashSet<>();
    private Map<String, ILinkObject> linkTables = new HashMap<>();

    public Session(long sessionId) {
        this(sessionId, "jdbc:mysql://localhost:3306/javastudy", "root", "root");

    }

    public Session(long sessionId, String dbAdress, String dbLogin, String dbPassword) {
        this.sessionId = sessionId;
        try {
            sqlConnection = DriverManager.getConnection(dbAdress,dbLogin, dbPassword);
        } catch (SQLException e) {
            throw new MyTournamentException("Error! Failed to establish database connection");
        }
    }

    public Set<String> getSessionTables() {
        return sessionTables;
    }

    public void dropTableSession() throws SQLException {
        for (String oneTableName : sessionTables) {
            Table.dropTable(getSqlConnection(), oneTableName);
        }
        Table.dropTable(getSqlConnection(),VARIALBE_TABLE_NAME + "_" + getSessionId());
    }

    public long getSessionId() {
        return sessionId;
    }

    public Connection getSqlConnection() {
        return sqlConnection;
    }

    private void saveDataAction(String inputData) throws SQLException {
        JSONObject dataJSON = new JSONObject(inputData);
        if ("table".equals(dataJSON.getString("type"))) {
            new Table(dataJSON, getSessionId()).update(getSqlConnection());
        } else {
            new Variable(dataJSON.getString("name"), Double.parseDouble(dataJSON.getString("value")), getSessionId())
                    .update(getSqlConnection());
        }
    }

    public String getResultByAction(String actionName, String inputData) throws SQLException {
        switch (actionName) {
            case "calculate":
                return getActionCalulate(inputData); // тестовая строка
            case "save_data":
                saveDataAction(inputData);
                return getMessageResult("Data is update");
            case "start_service":
                return getActiongStartService(inputData);
            case "start_and_calculate":
                JSONObject inputObj = new JSONObject(inputData);
                getActiongStartService(inputObj.getString("input_data"));
                return getActionCalulate(inputObj.getString("calculate"));
            case "close_session":
                SessionController.getInstance().closeSession(sessionId);
                return getMessageResult("Session is close");
            default:
                return "hello";
        }
    }

    private String getMessageResult(final String message) {
        return new JSONArray().put(new JSONObject() {{
            put("value", message);
        }}).toString();
    }

    private String getActionCalulate(String inputData) throws SQLException {
        JSONObject calculateObject = new JSONObject(inputData);
        JSONArray formuls = calculateObject.getJSONArray("formuls");
        if (calculateObject.keySet().contains("filters")) {
            JSONArray linkTables = calculateObject.getJSONArray("filters");
            for (int i = 0; i < linkTables.length(); i++) {
                JSONObject linkObjJSON = linkTables.getJSONObject(i);
                String name = linkObjJSON.getString("name") + "_" + getSessionId();
                if ("table".equals(linkObjJSON.getString("type"))) {
                    this.linkTables.put(name,
                            new LinkTable(linkObjJSON, getSessionTables(), getSessionId()));
                } else {
                    this.linkTables.put(name,
                            new LinkVariable(linkObjJSON.getString("source"),  getSessionId()));
                }
            }
        }
        JSONArray resultArr = new JSONArray();
        for (int i = 0; i < formuls.length(); i++) {
            resultArr.put(new Formula(formuls.getString(i), this, i).getJSONObject());
        }
        return resultArr.toString();
    }

    private void createVariableTable() throws SQLException {
        Table.dropTable(getSqlConnection(),VARIALBE_TABLE_NAME + "_" + getSessionId());
        try (Statement statement = getSqlConnection().createStatement()) {
            statement.execute("CREATE TABLE " + VARIALBE_TABLE_NAME + "_" + getSessionId() + " (" +
                    "var_name VARCHAR (100), " +
                    "var_value double precision)");
            sessionTables.add(VARIALBE_TABLE_NAME + "_" + getSessionId());
        }
    }

    private String getActiongStartService(String inputData) throws SQLException {
        JSONArray dataObjList = new JSONArray(inputData);
        createVariableTable();
        for (int i = 0; i < dataObjList.length(); i++) {
            JSONObject oneDataObj = dataObjList.getJSONObject(i);
            if ("table".equals(oneDataObj.getString("type"))) {
                Table oneTable = new Table(oneDataObj, sessionId);
                sessionTables.add(oneTable.getName());
                oneTable.insertInDB(getSqlConnection());
            } else {
                if ("long".equals(oneDataObj.getString("type")) || "double".equals(oneDataObj.getString("type"))) {
                    Variable variable = new Variable(oneDataObj.getString("name"),
                            Double.parseDouble(oneDataObj.get("value").toString()), getSessionId());
                    variable.insertInDB(getSqlConnection());
                }
            }
        }
        return new JSONArray().put(new JSONObject(){{
            put("sessionId", String.valueOf(getSessionId()));
        }}).toString();
    }

    private IDataObject getLinkDataObj(String objectName) throws SQLException {
        ILinkObject linkTable = linkTables.get(objectName);
        return linkTable.getSourceObject(getSqlConnection());
    }

    public IDataObject getDataObject(String name) throws SQLException {
        String objectName = name + "_" + getSessionId();
        if (getSessionTables().contains(objectName)) {
            try (PreparedStatement ps = getSqlConnection().prepareStatement(
                    "select * from " + objectName
            )) {
                return new Table(ps.executeQuery());
            }
        } else {
            if (linkTables.containsKey(objectName)) {
                return getLinkDataObj(objectName);
            } else {
                try (PreparedStatement ps = getSqlConnection().prepareStatement(
                        "select * from " + VARIALBE_TABLE_NAME + "_" + getSessionId() + " where var_name = ?"
                )) {
                    ps.setString(1, name);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return new Variable(rs);
                    }
                }
            }
        }
        throw new MyTournamentException("Error! Broken operator in formula");
    }

}
