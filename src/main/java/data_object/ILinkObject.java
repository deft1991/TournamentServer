package data_object;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by PC on 20.07.2017.
 */
public interface ILinkObject {

    public String getSql();
    public IDataObject getSourceObject(Connection connection) throws SQLException;
}
