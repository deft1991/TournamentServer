package data_object;

import session_tools.Session;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by PC on 20.07.2017.
 */
public class LinkVariable implements ILinkObject{

    private String sourceVarName;
    private long sessionId;

    public LinkVariable(String sourceVarName, long sessionId) {
        this.sourceVarName = sourceVarName;
        this.sessionId = sessionId;
    }

    @Override
    public String getSql() {
        return "SELECT * from " + Session.VARIALBE_TABLE_NAME + "_" + sessionId + " where var_name = ?";
    }

    @Override
    public IDataObject getSourceObject(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(getSql())) {
            ps.setString(1, sourceVarName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Variable(rs);
            }
        }
        return null;
    }
}
