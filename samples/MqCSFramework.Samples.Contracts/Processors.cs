namespace MqCSFramework.Samples.Contracts;

/// <summary>
/// Processor contract interface for standard order messages.
/// Referenced by both sender and consumer for compile-time type safety.
/// </summary>
public interface IOrderProcessor : IMessageProcessor<OrderMessage>;

/// <summary>
/// Processor contract interface for RPC stock checks.
/// Referenced by both sender and consumer for compile-time type safety.
/// </summary>
public interface IStockProcessor : IRpcProcessor<StockRequest, StockResponse>;
