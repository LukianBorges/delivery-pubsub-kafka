package br.com.delivery;

import com.delivery.OrderEvent;

public interface OrderMessageProducer {
    void enviarPedido(OrderEvent pedido) throws Exception;
}