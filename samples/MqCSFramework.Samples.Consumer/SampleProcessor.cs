using MqCSFramework.Abstractions.Models;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

/// <summary>
/// Sample RPC processor implementation. Receives a SampleRequest and returns a SampleResponse.
/// </summary>
public class SampleProcessor : ISampleProcessor
{
    public Task<SampleResponse> ProcessAsync(SampleRequest request, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"[Consumer] Received request: Name={request.Name}, Value={request.Value}, MessageId={context.MessageId}");

        var response = new SampleResponse(
            Result: $"Hello {request.Name}, your value is {request.Value * 2}",
            ProcessedAt: DateTimeOffset.UtcNow);

        Console.WriteLine($"[Consumer] Sending response: {response.Result}");

        return Task.FromResult(response);
    }
}
