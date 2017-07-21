package data_object;

import org.json.JSONObject;
import session_tools.Session;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * Created by PC on 16.07.2017.
 */
public interface IDataObject {

    public void insertInDB(Connection connection) throws SQLException;
    public void update(Connection connection) throws SQLException;
    public JSONObject getJSONObject();
    public String getName();
    public IDataObject getSumResult(IDataObject operandTwo, int resultIndex);
    public IDataObject getSubstractResult(IDataObject operandTwo, int resultIndex);
    public IDataObject getMultiplicationResult(IDataObject operandTwo, int resultIndex);
    public IDataObject getDividerResult(IDataObject operandTwo, int resultIndex);
    public void insertTempAsSorce(String name, Session session) throws SQLException;
}
