namespace MqCSFramework.Samples.Contracts;

public record OrderMessage(Guid OrderId, string CustomerName, decimal Amount, DateTimeOffset CreatedAt);

public record StockRequest(string Sku, int Quantity);

/// <summary>
/// RPC response for a stock check. Extends <see cref="RpcResponse"/>, so it carries the
/// operation outcome (Success/Error) alongside the payload. On a business failure the processor
/// returns this with <c>Error</c> set and leaves the payload members at their defaults.
/// </summary>
public record StockResponse : RpcResponse
{
    public bool Available { get; init; }
    public int RemainingStock { get; init; }
    public decimal UnitPrice { get; init; }
}
