using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace LODLCMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Course Migration";
            Console.Clear();
            Console.WriteLine("Sending Courses...");
            CreateHostBuilder(args).Build().Run();
            //Console.WriteLine("Hello World!");

        }


        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, collection) =>
            {
                collection.AddHostedService<KafkaHostedServiceProducer>();
            });
    }
    public class KafkaHostedServiceProducer : IHostedService
    {
        private readonly ILogger<KafkaHostedServiceProducer> _logger;
        private readonly IProducer<Null, string> _producer;

        public KafkaHostedServiceProducer(ILogger<KafkaHostedServiceProducer> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            for (var count = 0; count < 1000; count++)
            {
                var value = $"Total Student Migrated {count+ 1}";
                _logger.LogInformation(value);
                await _producer.ProduceAsync("testing", new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);
            }
            _producer.Flush(TimeSpan.FromSeconds(12));
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
