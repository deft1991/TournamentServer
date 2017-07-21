package data_object;

import org.json.JSONArray;
import org.json.JSONObject;
import session_tools.Session;
import tools.MyTournamentException;

import java.sql.*;
import java.util.*;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class Table implements IDataObject {

    private Set<Column> columns;
    private String tableName;
    private int lineCount;

    /*
    *      [{  "name": "table_1",
            "datatype": "table",
            "value": [
              [1, 2, 3],
              [4, 5, 6],
              [7, 8, 9]
            ],
            "columns": [
              {
                "name": "c1",
                "type": "number"
              },
              {
                "name": "c2",
                "type": "number"
              },
              {
                "name": "c3",
                "type": "number"
              }
            ]
          }]
    * */

    public Table(JSONObject jsonData, long sessionId) {
        this.tableName = jsonData.getString("name") + "_" + sessionId;
        JSONArray columnObjects = jsonData.getJSONArray("columns");
        JSONArray columnValues = jsonData.getJSONArray("value");
        this.columns = new LinkedHashSet<>();
        for (int i = 0; i < columnObjects.length(); i++) {
            addColumns(new Column(columnObjects.getJSONObject(i)));
        }
        this.lineCount = columnValues.length();
        for (int i = 0; i < lineCount; i++) {
            JSONArray lineValues = columnValues.getJSONArray(i);
            if (this.getColumns().size() != lineValues.length()) {
                throw new MyTournamentException("Error! the numbers of values don't match with numbers of columns " +
                        "in table " + getName());
            }
            Iterator<Column> it = this.getColumns().iterator();
            for (int k = 0; k < lineValues.length(); k++) {
                it.next().insertValue(lineValues.get(k));
            }
        }
    }

    public Table(ResultSet rs) throws SQLException {
        ResultSetMetaData rsMetaData = rs.getMetaData();
        this.tableName = rsMetaData.getTableName(1);
        this.columns = new LinkedHashSet<>();
        int columnCount = rsMetaData.getColumnCount();
        for (int i = 0; i < rsMetaData.getColumnCount(); i++) {
            //long = -5
            addColumns(new Column(rsMetaData.getColumnName(i + 1), rsMetaData.getColumnType(i + 1)));
        }
        while (rs.next()) {
            this.lineCount++;
            Iterator<Column> it = columns.iterator();
            for (int i = 1; i <= columnCount; i++) {
                it.next().insertValue(rs.getObject(i));
            }
        }
    }

    public Table(Matrix matrix, String tableName) {
        this.tableName = tableName;
        columns = new LinkedHashSet<>();
        lineCount = matrix.getValues().length;
        for (int i = 0; i < matrix.getValues()[0].length; i++) {
            Column oneColumn = new Column(tableName + "_" + i, matrix.getMatrixType());
            addColumns(oneColumn);
            for (int k = 0; k < lineCount; k++) {
                oneColumn.insertValue(matrix.getValues()[k][i]);
            }
        }
    }

    private void validateColumn(Column oneColumn) {
        if (columns != null && this.columns.contains(oneColumn)) {
            throw new MyTournamentException("Error! In table " + getName() + " alredy present " +
                    "column with name " + oneColumn.getName());
        }
    }

    private void addColumns(Column oneColumn) {
        validateColumn(oneColumn);
        this.columns.add(oneColumn);
    }

    public int getLineCount() {
        return lineCount;
    }

    public Set<Column> getColumns() {
        return columns;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public IDataObject getSumResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Table) {
            return new Table(new Matrix(this).getMatrixSum(new Matrix((Table) operandTwo)), "result_" + resultIndex);
        } else {
            throw new MyTournamentException("Error! Cannot sum matrices with variable");
        }
    }

    @Override
    public IDataObject getSubstractResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Table) {
            return new Table(new Matrix(this).getSubstractMatrix(new Matrix((Table) operandTwo)), "result_" + resultIndex);
        } else {
            throw new MyTournamentException("Error! Cannot substract matrices with variable");
        }
    }

    @Override
    public IDataObject getMultiplicationResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Table(new Matrix(this).getMultplicationMatrix(((Variable) operandTwo).getValue()), "result_" + resultIndex);
        } else {
            return new Table(new Matrix(this).getMultplicationMatrix(new Matrix((Table) operandTwo)), "result_" + resultIndex);
        }
    }

    @Override
    public IDataObject getDividerResult(IDataObject operandTwo, int resultIndex) {
        if (operandTwo instanceof Variable) {
            return new Table(new Matrix(this).getDividerMatrix(((Variable) operandTwo).getValue()), "result_" + resultIndex);
        } else {
            return new Table(new Matrix(this).getDividerMatrix(new Matrix((Table) operandTwo)), "result_" + resultIndex);
        }
    }

    @Override
    public void insertTempAsSorce(String name, Session session) throws SQLException {
        String tempTableName = name + "_" + session.getSessionId();
        this.dropTable(session.getSqlConnection(), tempTableName);
        this.createTable(session.getSqlConnection(), tempTableName);
        this.insertTableData(session.getSqlConnection(), tempTableName);
        session.getSessionTables().add(tempTableName);

    }

    @Override
    public void insertInDB(Connection connection) throws SQLException {
        dropTable(connection, tableName);
        createTable(connection);
        insertTableData(connection);
    }

    @Override
    public void update(Connection connection) throws SQLException {
        this.insertInDB(connection);
    }

    @Override
    public JSONObject getJSONObject() {
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("name", new StringBuilder(getName()).delete(getName().lastIndexOf("_"), getName().length()).toString());
        objectMap.put("type", "table");
        ArrayList[] lineValues = new ArrayList[lineCount];
        JSONArray columns = new JSONArray();
        for (int i = 0; i < lineCount; i++) {
            lineValues[i] = new ArrayList<>();
            Iterator<Column> it = this.columns.iterator();
            for (int k = 0; k < this.columns.size(); k++) {

                lineValues[i].add(it.next().getValues().get(i));
            }
        }
        objectMap.put("value", lineValues);
        for (Iterator<Column> it = this.columns.iterator(); it.hasNext(); ) {
            columns.put(it.next().getJSONObject());
        }
        objectMap.put("columns", columns);
        return new JSONObject(objectMap);
    }

    public static void dropTable(Connection connection, String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute("drop table if EXISTS " + tableName);
        }
    }

    private void createTable(Connection connection) throws SQLException {
        this.createTable(connection, getName());
    }

    private void createTable(Connection connection, String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            StringBuilder sql = new StringBuilder("CREATE  TABLE " + tableName);
            sql.append(" (");
            for (Iterator<Column> it = columns.iterator(); it.hasNext(); ) {
                sql.append(it.next().getSqlCreatColScript())
                        .append(it.hasNext() ? ", \n" : "");
            }
            sql.append(")");
            statement.execute(sql.toString());
        }
    }

    private void insertTableData(Connection connection, String tableName) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ")
                .append(tableName)
                .append(" (");
        for (Iterator<Column> it = columns.iterator(); it.hasNext(); ) {
            sql.append(it.next().getName())
                    .append(it.hasNext() ? ", " : "");
        }
        sql.append(") \n values (");
        for (int index = 0; index < lineCount; index++) {
            for (Iterator<Column> it = columns.iterator(); it.hasNext(); ) {
                sql.append(it.next().getValueSql(index))
                        .append(it.hasNext() ? ", " : "");
            }
            sql.append(index != lineCount - 1 ? "), \n (" : ")");
        }
        try(PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            ps.executeUpdate();
        }
    }

    private void insertTableData(Connection connection) throws SQLException {
        this.insertTableData(connection, this.tableName);
    }

    class Matrix {

        // [количество строк][количество столбцов]
        private Double[][] values;
        private int matrixType = Column.TYPE_LONG;

        public Matrix(Table table) {
            for (Column oneColumn : table.getColumns()) {
                if (oneColumn.getType() != Column.TYPE_LONG) {
                    if (oneColumn.getType() != Column.TYPE_DOUBLE) {
                        throw new MyTournamentException("Error! In table " + table.getName() + " not all columns with type long or double");
                    } else {
                        this.matrixType = Column.TYPE_DOUBLE;
                    }
                }
            }
            Set<Column> columns = table.getColumns();
            this.values = new Double[getLineCount()][columns.size()];
            for (int i = 0; i < values.length; i++) {
                Iterator<Column> it = columns.iterator();
                for (int k = 0; k < values[i].length; k++) {
                    values[i][k] = new Double(it.next().getValues().get(i).toString());
                }
            }
        }

        /*public static BigInteger determinant(final Double[][] matr) {

            int accuracy = 20;

            BigDecimal EPS = BigDecimal.valueOf(0.00000000001);

            int n = matr.length;
            BigDecimal[][] a = new BigDecimal[n][n];
            for (int i = 0; i < n; ++i)
                for (int j = 0; j < n; ++j) {
                    a[i][j] = new BigDecimal(matr[i][j]);
                    a[i][j].setScale(accuracy, BigDecimal.ROUND_HALF_UP);
                }

            BigDecimal det = new BigDecimal(1.0);
            det.setScale(accuracy, BigDecimal.ROUND_HALF_UP);

            for (int i = 0; i < n; ++i) {
                int k = i;
                for (int j = i + 1; j < n; ++j)
                    if (a[j][i].abs().compareTo(a[k][i].abs()) > 0)
                        k = j;
                if (a[k][i].abs().compareTo(EPS) < 0) {
                    det = new BigDecimal(0.0);
                    det.setScale(accuracy, BigDecimal.ROUND_HALF_UP);
                    break;
                }
                BigDecimal[] tmp = a[i];
                a[i] = a[k];
                a[k] = tmp;

                if (i != k)
                    det = det.divide(new BigDecimal(-1), accuracy, BigDecimal.ROUND_HALF_UP);
                det = det.multiply(a[i][i]);
                for (int j = i + 1; j < n; ++j)
                    a[i][j] = a[i][j].divide(a[i][i], accuracy, BigDecimal.ROUND_HALF_UP);
                for (int j = 0; j < n; ++j)
                    if (j != i && a[j][i].abs().compareTo(EPS) > 0)
                        for (int kk = i + 1; kk < n; ++kk) {
                            BigDecimal aikji = new BigDecimal(1.0);
                            aikji.setScale(accuracy, BigDecimal.ROUND_HALF_UP);
                            aikji = aikji.multiply(a[i][kk]);
                            aikji = aikji.multiply(a[j][i]);
                            aikji = aikji.multiply(new BigDecimal(-1));
                            a[j][kk] = a[j][kk].add(aikji);
                        }
            }

            det = det.abs();
            det = det.add(new BigDecimal(0.00001));
            return det.abs().toBigInteger();

        }
    */
        public int getMatrixType() {
            return matrixType;
        }

        public Double[][] getValues() {
            return values;
        }

        private Matrix(Double[][] values) {
            this.values = values;
        }

        public Matrix getMatrixSum(Matrix summand) {
            Double[][] summandValues = summand.getValues();
            if (isEqualMatrixSize(summandValues)) {
                throw new MyTournamentException("Dimensionality sum matrices are not the same");
            }
            Double[][] result = new Double[summandValues.length][summandValues[0].length];
            for (int i = 0; i < summandValues.length; i++) {
                for (int k = 0; k < summandValues[i].length; k++) {
                    result[i][k] = this.getValues()[i][k] + summandValues[i][k];
                }
            }
            return new Matrix(result);
        }

        private boolean isEqualMatrixSize(Double[][] matrixA) {
            return matrixA.length != this.getValues().length || matrixA[0].length != this.getValues()[0].length;
        }

        public Matrix getSubstractMatrix(Matrix subtrahend) {
            Double[][] subtrahendValues = subtrahend.getValues();
            if (isEqualMatrixSize(subtrahendValues)) {
                throw new MyTournamentException("Dimensionality deduction matrices are not the same");
            }
            Double[][] result = new Double[subtrahendValues.length][subtrahendValues[0].length];
            for (int i = 0; i < subtrahendValues.length; i++) {
                for (int k = 0; k < subtrahendValues[i].length; k++) {
                    result[i][k] = this.getValues()[i][k] - subtrahendValues[i][k];
                }
            }
            return new Matrix(result);
        }

        public Matrix getMultplicationMatrix(Double multiplier) {
            Double[][] res = new Double[getValues().length][getValues()[0].length];
            for (int i = 0; i < getValues().length; i++) {
                for (int k = 0; k < getValues()[0].length; k++) {
                    res[i][k] = getValues()[i][k] * multiplier;
                }
            }

            return new Matrix(res);
        }
        private void checkNaN(Double[][] matrix) {
            for (int i = 0; i < matrix.length; i++) {
                for (int k = 0; k < matrix[0].length; k++) {
                    if (matrix[i][k].isNaN()) {
                        throw new MyTournamentException("Error! It is impossible to find the inverse of a matrix");
                    }
                }
            }
        }

        public Matrix getMultplicationMatrix(Matrix multiplier) {
        /*int[][] mA =
                {{33,34,12},
                        {33,19,10},
                        {12,14,17},
                        {84,24,51},
                        {43,71,21}};

        int[][] mB =
                {{10,11,34,55},
                        {33,45,17,81},
                        {45,63,12,16}};

*/
            int m = getValues().length;
            int n = multiplier.getValues()[0].length;
            int o = multiplier.getValues().length;
            if (getValues()[0].length != o) {
                throw new MyTournamentException("Error! The number of columns of matrix " +
                        "A does not equal the number of rows of matrix B");
            }
            Double[][] res = new Double[m][n];

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    res[i][j] = new Double(0);
                    for (int k = 0; k < o; k++) {
                        res[i][j] += getValues()[i][k] * multiplier.getValues()[k][j];
                    }
                }
            }

        /*for (int i = 0; i < res.length; i++) {
            for (int j = 0; j < res[0].length; j++) {
                System.out.format("%6d ", res[i][j]);
            }
            System.out.println();
        }*/
            return new Matrix(res);
        }

        private Matrix getDividerMatrix(Double divider) {
            if (divider == 0) {
                throw new MyTournamentException("Cant devide by 0");
            }
            return getMultplicationMatrix(1 / divider);
        }

        private Matrix getDividerMatrix(Matrix matrix) {
            return getMultplicationMatrix(getInversionMatrix(matrix.getValues()));
        }

        private Matrix getInversionMatrix(Double[][] matrix) {
            int colCount = matrix.length;
            if (colCount != 0 && colCount != matrix[0].length && detMatrix(matrix).intValue() != 0) {
                throw new MyTournamentException("Cannot find revers matrices");
            }

            Double temp;
            Double[][] buf = new Double[colCount][colCount];
            for (int i = 0; i < colCount; i++) {
                for (int j = 0; j < colCount; j++) {
                    buf[i][j] = 0D;
                    if (i == j)
                        buf[i][j] = 1D;
                }
            }

//            checkNaN(matrix);
            for (int k = 0; k < colCount; k++) {
                temp = matrix[k][k];
                if (temp == 0)
                    throw new MyTournamentException("Error! It is impossible to find the inverse of a matrix");
                for (int j = 0; j < colCount; j++) {
                    matrix[k][j] /= temp;
                    buf[k][j] /= temp;
                }

                for (int i = k + 1; i < colCount; i++) {
                    temp = matrix[i][k];

                    for (int j = 0; j < colCount; j++) {
                        matrix[i][j] -= matrix[k][j] * temp;
                        buf[i][j] -= buf[k][j] * temp;
                    }
                }
            }

            for (int k = colCount - 1; k > 0; k--) {
                for (int i = k - 1; i >= 0; i--) {
                    temp = matrix[i][k];

                    for (int j = 0; j < colCount; j++) {
                        matrix[i][j] -= matrix[k][j] * temp;
                        buf[i][j] -= buf[k][j] * temp;
                    }
                }
            }

            for (int i = 0; i < colCount; i++) {
                for (int j = 0; j < colCount; j++) {
                    matrix[i][j] = buf[i][j];
                }
            }

            return new Matrix(matrix);
        }

        public Double detMatrix(Double[][] matrix){
            double calcResult = 0D;
            if (matrix.length == 2) {
                calcResult = matrix[0][0] * matrix[1][1] - matrix[1][0] * matrix[0][1];
            }
            else{
                int koeff;
                for (int i = 0; i < matrix.length; i++) {
                    if (i % 2 == 1) {  //я решил не возводить в степень, а просто поставить условие - это быстрее. Т.к. я раскладываю всегда по первой (читай - "нулевой") строке, то фактически я проверяю на четность значение i+0.
                        koeff =- 1;
                    }
                    else{
                        koeff = 1;
                    }
                    //собственно разложение:
                    calcResult += koeff * matrix[0][i] * this.detMatrix(this.GetMinor(matrix,0 ,i));
                }
            }

            //возвращаем ответ
            return calcResult;
        }

        private Double[][] GetMinor(Double[][] matrix, int row, int column){
            int minorLength = matrix.length - 1;
            Double[][] minor = new Double[minorLength][minorLength];
            int dI = 0;//эти переменные для того, чтобы "пропускать" ненужные нам строку и столбец
            int dJ;
            for (int i = 0; i <= minorLength; i++) {
                dJ = 0;
                for (int j = 0; j <= minorLength; j++) {
                    if (i == row) {
                        dI = 1;
                    }
                    else{
                        if(j == column) {
                            dJ = 1;
                        }
                        else{
                            minor[i - dI][j - dJ] = matrix[i][j];
                        }
                    }
                }
            }

            return minor;

        }

        @Override
        public String toString() {
            return super.toString();
        }
    }
}
