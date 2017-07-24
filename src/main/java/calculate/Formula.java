package calculate;

import data_object.IDataObject;
import org.json.JSONObject;
import session_tools.Session;
import tools.MyTournamentException;

import java.sql.SQLException;

/**
 *
 * Created by PC on 16.07.2017.
 */
public class Formula {

    public static final String SUBSTRACT = "-";
    public static final String SUM = "+";
    public static final String MULTIPLICATION = "*";
    public static final String DIVIDE = "/";
    public static final String TEMP = "->";

    private String formulaScript;
    private IDataObject result;
    private Session session;
    private int formulaIndex;

    public Formula(String formulaScript, Session session, int formulaIndex) throws SQLException {
        this.formulaScript = formulaScript;
        this.formulaIndex = formulaIndex;
        this.session = session;
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
        if (script.indexOf(DIVIDE) == index) {
            operator = DIVIDE;
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
                operandOne = session.getDataObject(script.substring(0, i));
                return getOperationResult(operandOne, operator, script.delete(0, i + operator.length()));
            }
        }
        throw new MyTournamentException("Error! Broken formula " + formulaScript);
    }

    private IDataObject getOperationResult(IDataObject firstOperand, String operator, StringBuilder script) throws SQLException {
        String nextOperator;
        for (int i = 0; i < script.length(); i++) {
            if ((nextOperator = getOperator(i, script.toString())) != null || i == script.length() - 1) {
                IDataObject result = getResult(firstOperand, script.substring(0, i + operator.length() - 1), operator);
                if (nextOperator != null) {
                    return getOperationResult(result, nextOperator, script.delete(0, i + nextOperator.length()));
                } else {
                    if (result != null) {
                        result.insertInDB(session.getSqlConnection());
                    }
                    return result;
                }
            }
        }
        throw new MyTournamentException("Error! Broken formula " + formulaScript);
    }

    private IDataObject getResult(IDataObject operandOne, String operandTwoName, String operator) throws SQLException {
        if (TEMP.equals(operator)) {
            operandOne.insertTempAsSorce(operandTwoName, session);
            return session.getDataObject(operandTwoName);
        }
        IDataObject operandTwo = session.getDataObject(operandTwoName);
        switch (operator) {
            case SUBSTRACT : return getSubstract(operandOne, operandTwo);
            case SUM : return getSum(operandOne, operandTwo);
            case MULTIPLICATION : return operandOne.getMultiplicationResult(operandTwo, formulaIndex);
            case DIVIDE : return operandOne.getDividerResult(operandTwo, formulaIndex);
        }
        return null;

    }

    private IDataObject getSum(IDataObject operandOne, IDataObject operandTwo) throws SQLException {
        if (operandOne.getClass().equals(operandTwo.getClass())) {
            return operandOne.getSumResult(operandTwo, formulaIndex);
        } else {
            throw new MyTournamentException("Error! Cannot sum matrices with variable");
        }
    }

    private IDataObject getSubstract(IDataObject operandOne, IDataObject operandTwo) throws SQLException {
        if (operandOne.getClass().equals(operandTwo.getClass())) {
            return operandOne.getSubstractResult(operandTwo, formulaIndex);
        } else {
            throw new MyTournamentException("Error! Cannot substract matrices with variable");
        }
    }

    public JSONObject getJSONObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("formula", formulaScript);
        jsonObject.put("result", result.getJSONObject());
        return jsonObject;
    }


}
