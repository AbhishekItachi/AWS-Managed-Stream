using Amazon.Kafka;
using Amazon.Kafka.Model;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace AmazonManagedStream
{
    public class KafkaProducerService
    {
        private readonly string accessKeyId;
        private readonly string secretAccessKey;
        private string bootStrapServer;
        private readonly string clusterArn;
        private readonly string topicName;
        private readonly short replicationFactor;
        private readonly int partitions;
        public KafkaProducerService(IConfiguration configuration)
        {
            accessKeyId = configuration["AccessKey"];
            secretAccessKey = configuration["SecretAccessKey"];
            clusterArn = configuration["ClusterArn"];
            topicName = configuration["Topic"];
            replicationFactor = Convert.ToInt16(configuration["ReplicationFactor"]);
            partitions = Convert.ToInt32(configuration["Partitions"]);
            GetsessionCredentialsAsync().GetAwaiter().GetResult();
            CreateKafkaConfigruations().GetAwaiter().GetResult();
            CreatekafkaTopicIfNotExists().GetAwaiter().GetResult();
        }

        private void OauthCallback(IClient client, string cfg)
        {
            try
            {
                long timeValue = 0;
                long currentTime = DateTime.UtcNow.ToUnixTimeMilliSeconds();
                AWSMSKAuthTokenGenerator mskAuthTokenGenerator = new AWSMSKAuthTokenGenerator();
                (var token, timeValue) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(() => GetsessionCredentialsAsync().Result, Amazon.RegionEndpoint.USEast1).Result;
                client.OAuthBearerSetToken(token, timeValue, "");
            }
            catch (Exception e)
            {
                client.OAuthBearerSetTokenFailure(e.ToString());
            }
        }
        private async Task CreateKafkaConfigruations()
        {
            GetBootstrapBrokersRequest getBootstrapBrokersRequest = new();
            getBootstrapBrokersRequest.ClusterArn = clusterArn;

            AmazonKafkaClient amazonKafkaClient = new(GetsessionCredentialsAsync().Result, Amazon.RegionEndpoint.USEast1);
            GetBootstrapBrokersResponse response = await amazonKafkaClient.GetBootstrapBrokersAsync(getBootstrapBrokersRequest);
            bootStrapServer = response.BootstrapBrokerStringPublicSaslIam;
        }

        private async Task CreatekafkaTopicIfNotExists()
        {
            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootStrapServer,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer
            };

            using (var adminClient = new AdminClientBuilder(adminClientConfig).SetOAuthBearerTokenRefreshHandler(OauthCallback).Build())
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                var topicsMetadata = metadata.Topics;
                var topicNames = metadata.Topics.Select(a => a.Topic).ToList();

                try
                {
                    if (!topicNames.Exists(x => x.Equals(topicName)))
                    {
                        var topicSpecification = new TopicSpecification
                        {
                            Name = topicName,
                            NumPartitions = partitions,
                            ReplicationFactor = replicationFactor
                        };


                        await adminClient.CreateTopicsAsync(new[] { topicSpecification });

                        Console.WriteLine("Topic created successfully.");
                    }
                    else
                    {
                        Console.WriteLine("Topic already exists.");
                    }
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic: {e.Results[0].Error.Reason}");
                }
                catch (Exception e)
                {

                }
            }
        }

        public async Task KafkaProducer()
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootStrapServer,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer
            };

            //Func<SessionAWSCredentials> tempCredentials = () => sessionAWSCredentials;
            var config = new AdminClientConfig
            {
                BootstrapServers = bootStrapServer,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer
            };
            // Callback to handle OAuth bearer token refresh.
            var producer = new ProducerBuilder<string, string>(producerConfig)
                                .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build();

            try
            {
                int i = 0;
                string guid = Guid.NewGuid().ToString();
                while (true)
                {
                    var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> 
                    { 
                        Value = "default message",
                        Headers = new Headers { { "x-correlation-id", Encoding.UTF8.GetBytes(guid) } },
                        Key = guid,
                    });

                    Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
                }
            }
            catch (ProduceException<string, string> e)
            {
                Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e.Message}");
            }
            finally
            {
                producer.Dispose();
            }
        }

        private async Task<SessionAWSCredentials> GetsessionCredentialsAsync()
        {
            using (var stsClient = new AmazonSecurityTokenServiceClient(accessKeyId, secretAccessKey))
            {
                var getSessionTokenRequest = new GetSessionTokenRequest
                {
                    DurationSeconds = 7200 // seconds
                };

                GetSessionTokenResponse sessionTokenResponse = await stsClient.GetSessionTokenAsync(getSessionTokenRequest);

                Credentials credentials = sessionTokenResponse.Credentials;

                var sessionAWSCredentials = new SessionAWSCredentials(credentials.AccessKeyId,
                                                                  credentials.SecretAccessKey,
                                                                  credentials.SessionToken);
                return sessionAWSCredentials;
            }
        }
    }
}
