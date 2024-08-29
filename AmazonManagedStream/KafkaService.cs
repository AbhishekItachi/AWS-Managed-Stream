using Amazon.Kafka;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonManagedStream
{
    public class KafkaService
    {
        private readonly string accessKeyId;
        private readonly string secretAccessKey;
        private readonly string bootStrapServer;

        public KafkaService(IConfiguration configuration)
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

            //Callback to handle OAuth bearer token refresh. It fetches the OAUTH Token from the AWSMSKAuthTokenGenerator class. 
            void OauthCallback(IClient client, string cfg)
            {
                try
                {
                    var (token, expiryMs) = mskAuthTokenGenerator.GenerateAuthTokenFromCredentialsProviderAsync(tempCredentials, Amazon.RegionEndpoint.EUWest2).Result;
                    client.OAuthBearerSetToken(token, expiryMs, "DummyPrincipal");
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
                var deliveryReport = await producer.ProduceAsync("test-topic", new Message<string, string> { Value = "Hello from .NET" });

                Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
            }
            catch (ProduceException<string, string> e)
            {
                Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
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

                GetSessionTokenResponse sessionTokenResponse =
                              await stsClient.GetSessionTokenAsync(getSessionTokenRequest);

                Credentials credentials = sessionTokenResponse.Credentials;

                var sessionCredentials =
                    new SessionAWSCredentials(credentials.AccessKeyId,
                                              credentials.SecretAccessKey,
                                              credentials.SessionToken);
                return sessionCredentials;
            }
        }

        private async Task GenerateAuthTokenAsync()
        {
            try
            {
                using (var stsClient = new AmazonSecurityTokenServiceClient(accessKeyId, secretAccessKey))
                {
                    AWSMSKAuthTokenGenerator mskAuthTokenGenerator = new AWSMSKAuthTokenGenerator(stsClient);
                    var (token, expiryMs) = await mskAuthTokenGenerator.GenerateAuthTokenAsync(Amazon.RegionEndpoint.EUWest2);
                }  
            }
            catch (Exception e) 
            { 
            
            }
        }

        private async Task<Credentials> GetTemporaryCredentialsAsync()
        {
            using (var stsClient = new AmazonSecurityTokenServiceClient(accessKeyId, secretAccessKey))
            {
                var getSessionTokenRequest = new GetSessionTokenRequest
                {
                    DurationSeconds = 7200 // seconds
                };

                GetSessionTokenResponse sessionTokenResponse =
                              await stsClient.GetSessionTokenAsync(getSessionTokenRequest);

                Credentials credentials = sessionTokenResponse.Credentials;
               
                return credentials;
            }
        }
    }
}
