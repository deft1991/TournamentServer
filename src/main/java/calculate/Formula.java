package calculate;

import data_object.IDataObject;
import data_object.Table;
import session_tools.Session;

import java.sql.SQLException;

/**
 *
 * Created by PC on 16.07.2017.
 */
public class Formula {

    public static final String SUBSTRACT = "-";
    public static final String SUM = "+";
    public static final String MULTIPLICATION = "*";
    public static final String TEMP = "=>";

    private String formulaScript;
    private IDataObject result;
    private Session session;
    private int formulaIndex;

    public Formula(String formulaScript, Session session, int formulaIndex) throws SQLException {
        this.formulaScript = formulaScript;
        this.formulaIndex = formulaIndex;
        result = parseFormula(new StringBuilder(formulaScript));
    }

    private String getOperator(int index, String script) {
        String operator = null;
        if (script.indexOf(SUBSTRACT) == index) {
            operator = SUBSTRACT;
        }
        if (script.indexOf(SUM) == index) {
            operator = SUM;
        }
        if (script.indexOf(MULTIPLICATION) == index) {
            operator = MULTIPLICATION;
        }
        if (script.indexOf(TEMP) == index) {
            operator = TEMP;
        }
        return operator;
    }

    private IDataObject parseFormula(StringBuilder script) throws SQLException {
        IDataObject operandOne;
        String operator;
        for (int i = 0; i < script.length(); i++) {
            if ((operator = getOperator(i, script.toString())) != null) {
                operandOne = session.getDataObject(script.substring(0, i + operator.length()));
                return getOperationResult(operandOne, operator, script.delete(0, i));
            }
        }
        throw new RuntimeException("Ошибка! Не верная формула");
    }

    private IDataObject getOperationResult(IDataObject firstOperand, String operator, StringBuilder script) throws SQLException {
        String nextOperator;
        for (int i = 0; i < script.length(); i++) {
            if ((nextOperator = getOperator(i, script.toString())) != null || i != script.length() - 1) {
                IDataObject result = getResult(firstOperand, script.substring(0, i), operator);
                if (nextOperator != null) {
                    return getOperationResult(result, nextOperator, script.delete(0, i + nextOperator.length()));
                } else {
                    return result;
                }
            }
        }
        throw new RuntimeException("Ошибка! Не верная формула");
    }

    private IDataObject getResult(IDataObject operandOne, String operandTwoName, String operator) throws SQLException {
        IDataObject operandTwo = session.getDataObject(operandTwoName);
        switch (operator) {
            case SUBSTRACT : return operandOne.getSubstractResult(operandTwo, formulaIndex);
            case SUM : return operandOne.getSumResult(operandTwo, formulaIndex);
            case MULTIPLICATION : return operandOne.getDividerResult(operandTwo, formulaIndex);
            case TEMP :
                operandOne.insertTempAsSorce(operandTwoName, session);
                return session.getDataObject(operandTwoName);
        }
        return null;

    }


}
