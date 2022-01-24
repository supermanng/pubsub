using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LODLCMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            // Console.WriteLine("Hello World!");
            Console.Title = "Student Migration";
            Console.WriteLine("Ongoing...");
            CreateHostBuilder(args).Build().Run();
        }
        private static IHostBuilder CreateHostBuilder(string[] args) =>
       Host.CreateDefaultBuilder(args)
       .ConfigureServices((context, collection) =>
       {
           collection.AddHostedService<KafkaHostedServiceConsumer>();
       });
    }
    public class KafkaHostedServiceConsumer : IHostedService
    {
        private readonly ILogger<KafkaHostedServiceConsumer> _logger;
        private readonly ClusterClient _cluster;

        public KafkaHostedServiceConsumer(ILogger<KafkaHostedServiceConsumer> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(new Configuration
            {
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }
        public async Task StartAsync(CancellationToken cancellationToken)
        {

            _cluster.ConsumeFromEarliest("testing");
            _cluster.MessageReceived += record =>
            {
                var msg = $"{ Encoding.UTF8.GetString(record.Value as byte[]) }";
                _logger.LogInformation($"Received: {msg} ");
            };
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }
}