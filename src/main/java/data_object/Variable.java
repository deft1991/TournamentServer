package data_object;

import org.json.JSONObject;
import session_tools.Session;

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
                "insert INTO " + Session.VARIALBE_TABLE_NAME + sessionId +
                        "(var_name, var_value) VALUES (?, ?)"
        )) {
            ps.setString(1, variableName);
            ps.setDouble(2, value);
            ps.executeUpdate();
        }
    }

    @Override
    public JSONObject getJSONObject() {
        return null;
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
            throw new RuntimeException("Ошибка! Нельзя суммировать матрицу с числом");
        }
    }

    @Override
    public IDataObject getSubstractResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Variable("result_" + resultIndex, getValue() - ((Variable) operandTwo).getValue());
        } else {
            throw new RuntimeException("Ошибка! Нельзя вычитать матрицу и число");
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
            return new Variable("result_" + resultIndex, getValue() /  ((Variable) operandTwo).getValue());
        } else {
            return operandTwo.getDividerResult(this, resultIndex);
        }
    }

    @Override
    public void insertTempAsSorce(String name, Session session) throws SQLException {
        try (PreparedStatement ps = session.getSqlConnection().prepareStatement(
                "insert into " + Session.VARIALBE_TABLE_NAME + session.getSessionId() + " (var_name, var_value) " +
                        "VALUES (?, ?)"
        )) {
            ps.setString(1, name);
            ps.setDouble(2, value);
            ps.executeUpdate();
        }
    }
}
