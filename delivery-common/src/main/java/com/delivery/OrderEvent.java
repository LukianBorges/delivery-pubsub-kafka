package com.delivery;

import io.swagger.v3.oas.annotations.media.Schema;

public class OrderEvent {
    @Schema(description = "ID do pedido gerado pelo servidor", accessMode = Schema.AccessMode.READ_ONLY)
    private String orderId;

    private String customerName;
    private String restaurant;

   
    private double amount;

    @Schema(description = "Status atualizado pelo servidor", accessMode = Schema.AccessMode.READ_ONLY)
    private String status;

    
    public OrderEvent() {}

    
    public OrderEvent(String orderId, String customerName, String restaurant, double amount, String status) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.restaurant = restaurant;
        this.amount = amount;
        this.status = status;
    }

    
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }
    public String getRestaurant() { return restaurant; }
    public void setRestaurant(String restaurant) { this.restaurant = restaurant; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
