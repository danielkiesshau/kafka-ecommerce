package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
//                    we are not caring about security here
//                    we are just practicing a concept using http as an entry point

            var userEmail = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, userEmail);


            try (var database = new OrdersDatabase())  {
                if (!database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order, new CorrelationId(NewOrderServlet.class.getSimpleName()));


                    System.out.println("New Order sent successfully");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New Order sent");
                    return;
                }

                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println("Old Order received");
            }
        } catch (ExecutionException | SQLException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
