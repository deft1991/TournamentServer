package servlet;

import org.json.JSONArray;
import org.json.JSONObject;
import session_tools.Session;
import session_tools.SessionController;
import tools.MyTournamentException;
import tools.Tools;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class TournamentServlet extends HttpServlet {

    public TournamentServlet() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        setAccessControlHeaders(resp);
        String actionName = req.getParameter("action");
        String data = req.getParameter("data");
        if (!Tools.isEmptyString(actionName)) {
            String sessionIdStr = req.getParameter("sessionId");
            long sessionId;
            if (Tools.isEmptyString(sessionIdStr) || "undefined".equalsIgnoreCase(sessionIdStr)) {
                sessionId = SessionController.getInstance().generateSessionId();
            } else {
                sessionId = Long.parseLong(sessionIdStr);
            }
            Session session = SessionController.getInstance().getSessionById(sessionId);
            PrintWriter out = resp.getWriter();
            String result;
            try {
                result = session.getResultByAction(actionName, data);
            } catch (MyTournamentException myE) {
                result = myE.getMessage();
            } catch (Exception e) {
                JSONArray errArr = new JSONArray();
                final String err  = e.getMessage();
                errArr.put(new JSONObject() {{
                    put("error", err);
                }});
                result = errArr.toString();
            }
            out.print(result);
        }

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }

    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        setAccessControlHeaders(resp);
        resp.setStatus(HttpServletResponse.SC_OK);
    }

    private void setAccessControlHeaders(HttpServletResponse resp) {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "GET");
    }
}
