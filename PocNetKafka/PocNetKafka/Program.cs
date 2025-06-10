using Confluent.Kafka;
using System;
using System.Text;

namespace PocNetKafka
{
    class Program
    {
        static string bootstrapServers = "localhost:29092";
        static string topic = "meu-topico";

        static async Task Main(string[] args)
        {
            Console.WriteLine("1 - Producer");
            Console.WriteLine("2 - Consumer");
            Console.Write("Escolha: ");
            var choice = Console.ReadLine();

            if (choice == "1")
            {
                await RunProducer();
            }
            else if (choice == "2")
            {
                RunConsumer();
            }
        }

        static async Task RunProducer()
        {
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            while (true)
            {
                Console.Write("Digite a mensagem (ou 'sair'): ");
                string input = Console.ReadLine();
                if (input?.ToLower() == "sair") break;

                var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = input });

                Console.WriteLine($"[✔] Enviado para {result.TopicPartitionOffset}");
            }
        }

        static void RunConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "grupo-consumidor-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(topic);
            Console.WriteLine("Consumidor iniciado. Pressione Ctrl+C para sair.");

            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"[✔] Mensagem recebida: {cr.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}
