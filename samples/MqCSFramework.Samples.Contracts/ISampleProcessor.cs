using MqCSFramework.Abstractions.Processor;

namespace MqCSFramework.Samples.Contracts;

/// <summary>
/// Shared contract interface for the sample RPC processor.
/// Referenced by both sender and consumer — the sender uses it for
/// compile-time type safety via SendAsync&lt;ISampleProcessor&gt;.
/// </summary>
public interface ISampleProcessor : IRpcProcessor<SampleRequest, SampleResponse>;
