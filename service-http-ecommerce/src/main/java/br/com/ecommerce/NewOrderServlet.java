package br.com.ecommerce;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
//                    we are not caring about security here
//                    we are just practicing a concept using http as a entry point
            var userEmail = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, userEmail);
            var email = new Email("br.com.ecommerce.Order in progress", "Thank you for your order! We are processing your order");

            orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order, new CorrelationId(NewOrderServlet.class.getSimpleName()));

            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, email, new CorrelationId(NewOrderServlet.class.getSimpleName()));

            System.out.println("New Order sent successfully");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New Order Sent");
        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
