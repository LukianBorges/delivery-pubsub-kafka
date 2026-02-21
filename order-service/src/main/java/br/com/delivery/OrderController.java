package br.com.delivery;

import com.delivery.OrderEvent;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pedidos")
@Tag(name = "Pedidos", description = "Endpoints para criação e acompanhamento de pedidos via Kafka")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @Operation(summary = "Cria um novo pedido")
    public OrderEvent criarPedido(@RequestBody OrderEvent pedido) throws Exception {
        return orderService.processarNovoPedido(pedido);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Consulta o status de um pedido")
    public String consultarStatus(@PathVariable String id) {
        return orderService.consultarStatus(id);
    }
}