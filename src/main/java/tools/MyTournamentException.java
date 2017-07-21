package tools;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * Created by PC on 19.07.2017.
 */
public class MyTournamentException extends RuntimeException {
    public MyTournamentException(final String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        final String message = super.getMessage();
        JSONArray errArr = new JSONArray();
        errArr.put(new JSONObject() {{
            put("error", message);
        }});
        return errArr.toString();
    }
}
