// See https://aka.ms/new-console-template for more information
using Amazon.Kafka.Model;
using AmazonManagedStream;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;


Console.WriteLine("Hello, World!");

try
{
    IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("settings.json") // Load test settings from .json file.
            .Build();
    IMemoryCache cache = new MemoryCache(new MemoryCacheOptions());
    KafkaProducerService kafkaService = new KafkaProducerService(configuration, cache);
    await kafkaService.KafkaProducer();
}
catch (Exception ex)
{

}

Console.WriteLine("Hello, World!");
