using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using System.Net;

namespace AmazonManagedStream
{
    public class KafkaProducerService
    {
        private readonly string accessKeyId;
        private readonly string secretAccessKey;
        private readonly string bootStrapServer;

        public KafkaProducerService(IConfiguration configuration)
        {
            accessKeyId = configuration["AccessKey"];
            secretAccessKey = configuration["SecretAccessKey"];
            bootStrapServer = configuration["BootstrapServers"];
        }

        public async Task KafkaConfiguration()
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootStrapServer,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer
            };

            Func<SessionAWSCredentials> tempCredentials = () => GetsessionCredentialsAsync().Result;
            AWSMSKAuthTokenGenerator mskAuthTokenGenerator = new AWSMSKAuthTokenGenerator();
            var config = new AdminClientConfig
            {
                BootstrapServers = bootStrapServer,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer
                , // AWS MSK IAM support: ScramSha256 or ScramSha512
                //SaslUsername = accessKeyId,
                //SaslPassword = secretAccessKey
            };
            // Callback to handle OAuth bearer token refresh.
            void OauthCallback(IClient client, string cfg)
            {
                try
                {
                    var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(tempCredentials, Amazon.RegionEndpoint.EUWest2).Result;
                    client.OAuthBearerSetToken(token, expiryMs, "");
                }
                catch (Exception e)
                {
                    client.OAuthBearerSetTokenFailure(e.ToString());
                }
            }

            var producer = new ProducerBuilder<string, string>(producerConfig)
                                .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build();
            try
            {
                int i = 0;
                while (true)
                {
                    i++;
                    var deliveryReport = await producer.ProduceAsync("demo-topic-test-2", new Message<string, string> { Value = "Hello from .NET " + i });

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

                var sessionCredentials = new SessionAWSCredentials(credentials.AccessKeyId,
                                                                  credentials.SecretAccessKey,
                                                                  credentials.SessionToken);
                return sessionCredentials;
            }
        }
    }
}

#region commented code
//using Amazon.Kafka;
//using Amazon.Runtime;
//using Amazon.SecurityToken;
//using Amazon.SecurityToken.Model;
//using AWS.MSK.Auth;
//using Confluent.Kafka;
//using Confluent.Kafka.Admin;
//using Microsoft.Extensions.Configuration;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace AmazonManagedStream
//{
//    public class KafkaProducerService
//    {
//        private readonly string accessKeyId;
//        private readonly string secretAccessKey;
//        private readonly string bootStrapServer;
//        private readonly SessionAWSCredentials sessionAWSCredentials;

//        public KafkaProducerService(IConfiguration configuration)
//        {
//            accessKeyId = configuration["AccessKey"];
//            secretAccessKey = configuration["SecretAccessKey"];
//            bootStrapServer = configuration["BootstrapServers"];
//            sessionAWSCredentials = GetsessionCredentialsAsync().Result;
//        }

//        public async Task KafkaConfiguration()
//        {
//            var producerConfig = new ProducerConfig
//            {
//                BootstrapServers = bootStrapServer,
//                SecurityProtocol = SecurityProtocol.SaslSsl,
//                SaslMechanism = SaslMechanism.OAuthBearer,

//                //SaslOAuthBearerTokenRefreshHandler
//                // AWS MSK IAM support: ScramSha256 or ScramSha512
//                //SaslUsername = accessKeyId,
//                //SaslPassword = secretAccessKey
//            };

//            Func<SessionAWSCredentials> tempCredentials = () => sessionAWSCredentials;
//            AWSMSKAuthTokenGenerator mskAuthTokenGenerator = new();

//            ////AmazonKafkaClient amazonKafkaClient = new AmazonKafkaClient(sessionAWSCredentials, Amazon.RegionEndpoint.EUWest2);
//            var config = new AdminClientConfig
//            {
//                BootstrapServers = bootStrapServer,
//                SecurityProtocol = SecurityProtocol.SaslSsl,
//                SaslMechanism = SaslMechanism.OAuthBearer
//                , // AWS MSK IAM support: ScramSha256 or ScramSha512
//                //SaslUsername = accessKeyId,
//                //SaslPassword = secretAccessKey
//                //SaslOauthbearerClientId = accessKeyId,
//                //SaslOauthbearerClientSecret = secretAccessKey
//            };
//            using (var adminClient = new AdminClientBuilder(config).SetOAuthBearerTokenRefreshHandler(OauthCallback).Build())
//            {
//                try
//                {
//                    var topicSpecification = new TopicSpecification
//                    {
//                        Name = "dot-net-topic-2",
//                        NumPartitions = 1,
//                        ReplicationFactor = 2
//                    };

//                    await adminClient.CreateTopicsAsync(new[] { topicSpecification });

//                    Console.WriteLine("Topic created successfully.");
//                }
//                catch (CreateTopicsException e)
//                {
//                    Console.WriteLine($"An error occurred creating topic: {e.Results[0].Error.Reason}");
//                }
//            }

//            // Callback to handle OAuth bearer token refresh.
//            void OauthCallback(IClient client, string cfg)
//            {
//                try
//                {
//                    //var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(tempCredentials, Amazon.RegionEndpoint.EUWest2).Result;
//                    var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(tempCredentials, Amazon.RegionEndpoint.EUWest2).Result;
//                    client.OAuthBearerSetToken(token, expiryMs, "DummyPrincipal");
//                }
//                catch (Exception e)
//                {
//                    client.OAuthBearerSetTokenFailure(e.ToString());
//                }
//            }

//            var producer = new ProducerBuilder<string, string>(producerConfig)
//            //.Build();
//            .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build();
//            //.SetOAuthBearerTokenRefreshHandler((producer, producerConfig) =>
//            //{
//            //    //var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(tempCredentials, Amazon.RegionEndpoint.EUWest2).Result;
//            //    var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenAsync(Amazon.RegionEndpoint.EUWest2).Result;
//            //    producer.OAuthBearerSetToken(token, expiryMs, null);
//            //}).Build();
//            try
//            {
//                int i = 0;
//                while (true)
//                {
//                    i++;
//                    var deliveryReport = await producer.ProduceAsync("dot-net-topic", new Message<string, string> { Value = "Hello from .NET " + i });

//                    Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
//                }
//            }
//            catch (ProduceException<string, string> e)
//            {
//                Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine($"Unexpected error: {e.Message}");
//            }
//            finally
//            {
//                producer.Dispose();
//            }
//        }

//        private async Task<SessionAWSCredentials> GetsessionCredentialsAsync()
//        {
//            using (var stsClient = new AmazonSecurityTokenServiceClient(accessKeyId, secretAccessKey))
//            {
//                var getSessionTokenRequest = new GetSessionTokenRequest
//                {
//                    DurationSeconds = 7200 // seconds
//                };

//                GetSessionTokenResponse sessionTokenResponse = await stsClient.GetSessionTokenAsync(getSessionTokenRequest);

//                Credentials credentials = sessionTokenResponse.Credentials;

//                var sessionCredentials = new SessionAWSCredentials(credentials.AccessKeyId,
//                                                                  credentials.SecretAccessKey,
//                                                                  credentials.SessionToken);
//                return sessionCredentials;
//            }
//        }
//    }
//}
#endregion
