package data_object;

import org.json.JSONArray;
import org.json.JSONObject;
import tools.MyTournamentException;
import tools.Tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 * Created by PC on 17.07.2017.
 */
public class LinkTable {

    /*
    * "filters":[
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
 ]*/

    private String sourceTableName;
    private List<String> filters = new ArrayList<>();

    public LinkTable(JSONObject linkObject, Set<String> sessionTables, long sessionId) {
        this.sourceTableName = linkObject.getString("source") + "_" + sessionId;
        if (sessionTables.contains(this.sourceTableName)) {
            JSONArray filters = linkObject.getJSONArray("filter");
            for (int i = 0; i < filters.length(); i++) {
                this.filters.add(getFilterFromJSON(filters.getJSONObject(i)));
            }
        } else {
            throw new MyTournamentException("Ошибка! В формуле указана не существующая таблица " + this.sourceTableName);
        }
    }

    private String getFilterFromJSON(JSONObject filterJSON) {
        String fieldName = filterJSON.getString("name");
        String operator = filterJSON.getString("operation");
        if (Tools.isEmptyString(operator)) {
            operator = "like";
        }
        return "(" + fieldName + " " + operator + " " + getSqlValue(filterJSON.getString("value"),
                filterJSON.getString("type"), "like".equals(operator)) + ")";
    }

    private String getSqlValue(String value, String type, boolean isLikeFilter) {
        switch (type) {
            case "long":
            case "boolean":
            case "double": return value;
            case "date":
                if (isLikeFilter) {
                    throw new MyTournamentException("Ошибка! Фильтрация типа like не совместима с типом date");
                }
            case "string": return "'" + (isLikeFilter ? "%" + value + "%" : value) + "'";
            default: throw new MyTournamentException("Не известный тип колонки");
        }
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<String> getFilters() {
        return filters;
    }

    public String getSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("select * from ")
                .append(sourceTableName)
                .append(" where ");
        for (int i = 0; i < filters.size(); i++) {
            sql.append(filters.get(i));
            if (i != filters.size() - 1) {
                sql.append(" and ");
            }
        }
        return sql.toString();
    }
}
