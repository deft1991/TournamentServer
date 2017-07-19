package session_tools;

import calculate.Formula;
import data_object.IDataObject;
import data_object.LinkTable;
import data_object.Table;
import data_object.Variable;
import org.json.JSONArray;
import org.json.JSONObject;

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
    private Map<String, LinkTable> linkTables = new HashMap<>();

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
            case "calculate" : return getActionCalulate(getTestCalculation()); // тестовая строка
            case "get_table" : return getActionHello();
            case "start_service" : return getActiongStartService(inputData);
            case "close_session" :
                SessionController.getInstance().closeSession(sessionId);
                return "session close, all tables are droped";
            default: return "hello";
        }
    }

    /*
    * {
 "uid": "d4dca8c7-5b65-11e2-bbf7-005056876601",
 "formuls":["t11+a2->temp1", "t->temp2"],
 "filters":[
  {
   "table" : "t11",
   "source" : "t1",

   "filter": [
    {"name":"Name", "operation":"", "value": "a"},
    {"name":"Salary", "operation":"<", "value": "500"}
   ]
  },
  {
   "table" : "t",
   "filter": [
    {"name":"Name", "operation":">", "value": "afd"},
    {"name":"Salary", "operation":"=", "value": "30"}
   ]
  }
 ]
}
    * */

    private String getTestCalculation() {
        return "{\n" +
                " \"formuls\":[\"t11+table_1->temp1\", \"t->temp2\"],\n" +
                " \"filters\":[\n" +
                "  {\n" +
                "   \"table\" : \"t11\",\n" +
                "   \"source\" : \"table_1\",\n" +
                "\n" +
                "   \"filter\": [\n" +
                "    {\"name\":\"c1\", \"operation\":\">\", \"value\": \"0\", \"type\": \"long\"},\n" +
                "    {\"name\":\"c2\", \"operation\":\"<\", \"value\": \"500\", \"type\": \"long\"}\n" +
                "   ]\n" +
                "  },\n" +
                "  {\n" +
                "   \"table\" : \"t\",\n" +
                "   \"source\" : \"table_1\", \n" +
                "   \"filter\": [\n" +
                "    {\"name\":\"c1\", \"operation\":\">\", \"value\": \"2\", \"type\": \"long\"},\n" +
                "    {\"name\":\"c2\", \"operation\":\"=\", \"value\": \"5\", \"type\": \"long\"}\n" +
                "   ]\n" +
                "  }\n" +
                " ]\n" +
                "}";
    }


    private String getActionCalulate(String inputData) throws SQLException {
        JSONObject calculateObject = new JSONObject(inputData);
        JSONArray formuls = calculateObject.getJSONArray("formuls");
        JSONArray linkTables = calculateObject.getJSONArray("filters");
        for (int i = 0; i < linkTables.length(); i++) {
            JSONObject linkTableJSON = linkTables.getJSONObject(i);
            this.linkTables.put(linkTableJSON.getString("table") + "_" + getSessionId(),
                    new LinkTable(linkTableJSON, getSessionTables(), getSessionId()));
        }
        JSONArray resultArr = new JSONArray();
        for (int i = 0; i < formuls.length(); i++) {
            resultArr.put(new Formula(formuls.getString(i), this, i).getJSONObject());
        }
        return resultArr.toString();
    }

    private String getActionHello() {
        return "hello";
    }

    private void createVariableTable() throws SQLException {
        try (Statement statement = getSqlConnection().createStatement()) {
            statement.execute("CREATE TABLE " + VARIALBE_TABLE_NAME + getSessionId() + "(" +
                    "var_name VARCHAR (100), " +
                    "var_value DOUBLE)");
        }
    }

    private String getActiongStartService(String inputData) throws SQLException {
        JSONArray dataObjList = new JSONArray(inputData);
        createVariableTable();
        for (int i = 0; i < dataObjList.length(); i++) {
            JSONObject oneDataObj = dataObjList.getJSONObject(i);
            if ("table".equals(oneDataObj.getString("datatype"))) {
                Table oneTable = new Table(oneDataObj, sessionId);
                sessionTables.add(oneTable.getName());
                oneTable.insertInDB(getSqlConnection());
            } else {
                Variable variable = new Variable(oneDataObj.getString("name"),
                        Double.parseDouble(oneDataObj.getString("value")), getSessionId());
                variable.insertInDB(getSqlConnection());
            }
        }
        return String.valueOf(sessionId);
    }

    private Table getTableFromLink(String objectName) throws SQLException {
        LinkTable linkTable = linkTables.get(objectName);
        try (PreparedStatement ps = getSqlConnection().prepareStatement(linkTable.getSql())) {
            return new Table(ps.executeQuery());
        }
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
                return getTableFromLink(objectName);
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
        }
        throw new RuntimeException("Ошибка! Не верно указан оператор");
    }

}
