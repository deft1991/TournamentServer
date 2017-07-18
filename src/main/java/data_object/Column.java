package data_object;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class Column {

    public static final int TYPE_STRING = 1;
    public static final int TYPE_LONG = 0;
    public static final int TYPE_DATE = 2;
    public static final int TYPE_BOOLEAN = 3;
    public static final int TYPE_DOUBLE = 4;

    private String name;
    private List<Object> values;
    private int type;

    public Column(JSONObject columnJSON) {
        this(columnJSON.getString("name"), getColumnType(columnJSON.getString("type")));
    }

    public Column(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void insertValue(Object value) {
        if (values == null) {
            values = new ArrayList<>();
        }
        values.add(value);
    }

    public String getName() {
        return name;
    }

    public List<Object> getValues() {
        return values;
    }

    private static int getColumnType(String typeStr) {
        switch (typeStr) {
            case "long": return TYPE_LONG;
            case "double": return TYPE_DOUBLE;
            case "string": return TYPE_STRING;
            case "date": return TYPE_DATE;
            case "boolean": return TYPE_BOOLEAN;
            default: throw new RuntimeException("Не известный тип колонки");
        }
    }

    private String getSqlType() {
        switch (type) {
            case TYPE_LONG: return " bigint";
            case TYPE_STRING : return " varchar(100)";
            case TYPE_DOUBLE : return " double precision";
            case TYPE_DATE : return " date";
            case TYPE_BOOLEAN : return " integer(1)";
            default: return "";
        }
    }

    public String getSqlCreatColScript() {
        return new StringBuilder().append(name)
                .append(getSqlType()).toString();
    }

    public String getValueSql(int index) {
        switch (type) {
            case TYPE_BOOLEAN:
            case TYPE_LONG:
            case TYPE_DOUBLE: return String.valueOf(values.get(index));
            case TYPE_STRING :
            case TYPE_DATE : return "'" + values.get(index).toString() + "'";
            default: return "";
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Column && ((Column) obj).getName().equals(this.getName());
    }
}
