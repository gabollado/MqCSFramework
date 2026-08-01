namespace MqCSFramework.Samples.Contracts;

public record OrderMessage(Guid OrderId, string CustomerName, decimal Amount, DateTimeOffset CreatedAt);

public record StockRequest(string Sku, int Quantity);
public record StockResponse(bool Available, int RemainingStock, decimal UnitPrice);
