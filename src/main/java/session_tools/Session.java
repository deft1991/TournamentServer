package session_tools;

import calculate.Formula;
import data_object.IDataObject;
import data_object.LinkTable;
import data_object.Table;
import data_object.Variable;
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
    private Map<String, LinkTable> linkTables = new HashMap<>();

    public Session(long sessionId) {
        this(sessionId, "jdbc:mysql://localhost:3306/javastudy", "root", "root");

    }

    public Session(long sessionId, String dbAdress, String dbLogin, String dbPassword) {
        this.sessionId = sessionId;
        try {
            sqlConnection = DriverManager.getConnection(dbAdress,dbLogin, dbPassword);
        } catch (SQLException e) {
            throw new MyTournamentException("Ошибка! Не удалось установить соединение с базой данных");
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
            case "calculate" : return getActionCalulate(inputData); // тестовая строка
            case "save_table" :
                new Table(new JSONObject(inputData), getSessionId()).insertInDB(getSqlConnection());
                return "Таблица успешно перезаписана";
            case "start_service" : return getActiongStartService(inputData);
            case "start_and_calculate" :
                JSONObject inputObj = new JSONObject(inputData);
                getActiongStartService(inputObj.getString("input_data"));
                return getActionCalulate(inputObj.getString("calculate"));
            case "close_session" :
                SessionController.getInstance().closeSession(sessionId);
                return "Сессия закрыта, все таблицы удалены";
            default: return "hello";
        }
    }

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
        if (calculateObject.keySet().contains("filters")) {
            JSONArray linkTables = calculateObject.getJSONArray("filters");
            for (int i = 0; i < linkTables.length(); i++) {
                JSONObject linkTableJSON = linkTables.getJSONObject(i);
                this.linkTables.put(linkTableJSON.getString("table") + "_" + getSessionId(),
                        new LinkTable(linkTableJSON, getSessionTables(), getSessionId()));
            }
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
                Variable variable = new Variable(oneDataObj.getString("name"),
                        Double.parseDouble(oneDataObj.get("value").toString()), getSessionId());
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
        throw new MyTournamentException("Ошибка! Не верно указан оператор");
    }

}
