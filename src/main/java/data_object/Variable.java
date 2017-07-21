package data_object;

import org.json.JSONObject;
import session_tools.Session;
import tools.MyTournamentException;
import tools.Tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class Variable implements IDataObject {

    private String variableName;
    private Double value;
    private Long sessionId;


    public Variable(ResultSet rs) throws SQLException {
        this(rs.getString("var_name"), rs.getDouble("var_value"));
    }

    public Variable(String variableName, Double value) {
        this(variableName, value, null);
    }

    public Variable(String variableName, Double value, Long sessionId) {
        this.value = value;
        this.variableName = variableName;
        this.sessionId = sessionId;
    }

    @Override
    public void insertInDB(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "insert INTO " + Session.VARIALBE_TABLE_NAME + "_" + sessionId +
                        "(var_name, var_value) VALUES (?, ?)"
        )) {
            ps.setString(1, variableName);
            ps.setDouble(2, value);
            ps.executeUpdate();
        }
    }

    @Override
    public void update(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE " + Session.VARIALBE_TABLE_NAME + "_" + sessionId +
                        " set var_value = ? where var_name = ?"
        )) {
            ps.setDouble(1, value);
            ps.setString(2, variableName);
            ps.executeUpdate();
        }
    }

    @Override
    public JSONObject getJSONObject() {
        JSONObject result = new JSONObject() {{
            put("name", variableName);
            put("value", value);
            put("type", "double");
        }};

        return result;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public String getName() {
        return variableName;
    }

    @Override
    public IDataObject getSumResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Variable("result_" + resultIndex, getValue() + ((Variable) operandTwo).getValue());
        } else {
            throw new MyTournamentException("Error! Cannot sum matrices with variable");
        }
    }

    @Override
    public IDataObject getSubstractResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Variable("result_" + resultIndex, getValue() - ((Variable) operandTwo).getValue());
        } else {
            throw new MyTournamentException("Error! Cannot substract matrices with variable");
        }
    }

    @Override
    public IDataObject getMultiplicationResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Variable("result_" + resultIndex, getValue() *  ((Variable) operandTwo).getValue());
        } else {
            return operandTwo.getMultiplicationResult(this, resultIndex);
        }
    }

    @Override
    public IDataObject getDividerResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            if (((Variable) operandTwo).getValue() == 0 )
                throw new MyTournamentException("Cant devide by 0");
                return new Variable("result_" + resultIndex, getValue() / ((Variable) operandTwo).getValue());
        } else {
            return operandTwo.getDividerResult(this, resultIndex);
        }
    }

    @Override
    public void insertTempAsSorce(String name, Session session) throws SQLException {
        try (PreparedStatement ps = session.getSqlConnection().prepareStatement(
                "insert into " + Session.VARIALBE_TABLE_NAME + "_" + session.getSessionId() + " (var_name, var_value) " +
                        "VALUES (?, ?)"
        )) {
            ps.setString(1, name);
            ps.setDouble(2, value);
            ps.executeUpdate();
        }
    }
}
