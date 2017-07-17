package calculate;

import data_object.Column;
import data_object.Table;

import java.util.Set;

/**
 *
 * Created by PC on 15.07.2017.
 */
public class Matrix {

    /*// [количество строк][количество столбцов]
    private Double[][] values;
    private int matrixType = Column.TYPE_LONG;

    public Matrix(Table table) {
        for (Column oneColumn : table.getColumns()) {
            if (oneColumn.getType() != Column.TYPE_LONG) {
                if (oneColumn.getType() != Column.TYPE_DOUBLE) {
                    throw new RuntimeException("Ошибка! В таблице " + table.getName() + " не все колонки числовые");
                } else {
                    this.matrixType = Column.TYPE_DOUBLE;
                }
            }
        }
        Set<Column> columns = table.getColumns();
        this.values = new Double[table.getLineCount()][columns.size()];
        for (int i = 0; i < values.length; i++) {
            for (int k = 0; k < values[i].length; k++) {
                values[i][k] = (Double) columns.iterator().next().getValues().get(k);
            }
        }
    }

    *//*public static BigInteger determinant(final Double[][] matr) {

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
*//*
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
            throw new RuntimeException("Размерность слогаемых матриц не совпадает");
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
            throw new RuntimeException("Размерность вычитаемых матриц не совпадает");
        }
        Double[][] result = new Double[subtrahendValues.length][subtrahendValues[0].length];
        for (int i = 0; i < subtrahendValues.length; i++) {
            for (int k = 0; k < subtrahendValues[i].length; k++) {
                result[i][k] = this.getValues()[i][k] - subtrahendValues[i][k];
            }
        }
        return new Matrix(result);
    }
    public Matrix getMultplicationMatrix(Matrix multiplier) {
        *//*int[][] mA =
                {{33,34,12},
                        {33,19,10},
                        {12,14,17},
                        {84,24,51},
                        {43,71,21}};

        int[][] mB =
                {{10,11,34,55},
                        {33,45,17,81},
                        {45,63,12,16}};

*//*
        int m = getValues().length;
        int n = multiplier.getValues()[0].length;
        int o = multiplier.getValues().length;
        Double[][] res = new Double[m][n];

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < o; k++) {
                    res[i][j] += getValues()[i][k] * multiplier.getValues()[k][j];
                }
            }
        }

        *//*for (int i = 0; i < res.length; i++) {
            for (int j = 0; j < res[0].length; j++) {
                System.out.format("%6d ", res[i][j]);
            }
            System.out.println();
        }*//*
        return new Matrix(res);
    }

    private Matrix getInversionMatrix(Double[][] matrix) {
        int colCount = matrix.length;
        if (colCount != 0 && colCount != matrix[0].length && detMatrix(matrix).intValue() != 0) {
            throw new RuntimeException("Для заданной матрици не существует обратной");
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

        for (int k = 0; k < colCount; k++) {
            temp = matrix[k][k];

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

    public Object[][] getMatrixValues() {
        if (matrixType != Column.TYPE_DOUBLE) {
            Integer[][] result = new Integer[getValues().length][getValues()[0].length];
            for (int i = 0; i < getValues().length; i++) {
                for (int k = 0; k < getValues().length; k++) {
                    result[i][k] = getValues()[i][k].intValue();
                }
            }
            return result;
        }

        return getValues();
    }

    @Override
    public String toString() {
        return super.toString();
    }*/
}
