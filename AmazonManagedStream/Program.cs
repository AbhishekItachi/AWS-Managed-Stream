// See https://aka.ms/new-console-template for more information
using Amazon.Kafka.Model;
using AmazonManagedStream;
using Microsoft.Extensions.Configuration;


Console.WriteLine("Hello, World!");

try
{
    IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("settings.json") // Load test settings from .json file.
            .Build();
    KafkaProducerService kafkaService = new KafkaProducerService(configuration);
    await kafkaService.KafkaProducer();
}
catch (Exception ex)
{

}

Console.WriteLine("Hello, World!");
