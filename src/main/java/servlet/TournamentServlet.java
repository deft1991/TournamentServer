package servlet;

import session_tools.Session;
import session_tools.SessionController;
import tools.Tools;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

/**
 *
 * Created by PC on 13.07.2017.
 */
public class TournamentServlet extends HttpServlet {

    public TournamentServlet() {
        try {
            Class.forName("org.mysql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String actionName = req.getParameter("action");
        //String data = req.getParameter("data");
        String data = "[{  \"name\": \"table_1\",\n" +
                "            \"datatype\": \"table\",\n" +
                "            \"value\": [\n" +
                "              [1, 2, 3],\n" +
                "              [4, 5, 6],\n" +
                "              [7, 8, 9]\n" +
                "            ],\n" +
                "            \"columns\": [\n" +
                "              {\n" +
                "                \"name\": \"c1\",\n" +
                "                \"type\": \"long\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"c2\",\n" +
                "                \"type\": \"long\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"c3\",\n" +
                "                \"type\": \"long\"\n" +
                "              }\n" +
                "            ]\n" +
                "          }]";
        String sessionIdStr = req.getParameter("sessionId");
        long sessionId;
        if (Tools.isEmptyString(sessionIdStr)) {
            sessionId = SessionController.getInstance().generateSessionId();
        } else {
            sessionId = Long.parseLong(sessionIdStr);
        }
        Session session = SessionController.getInstance().getSessionById(sessionId);
        PrintWriter out = resp.getWriter();
        String result;
        try {
            result = session.getResultByAction(actionName, data);
        } catch (Exception e) {
            result = "Произошла какая-то беда";
        }
        out.print(result);

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }
}
