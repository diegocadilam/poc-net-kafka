using Confluent.Kafka;
using System;
using System.Text;
using System.Text.Json;

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
                var produto = new Product();
                produto.Id = 1;

                Console.Write("Digite o nome do produto (ou sair): ");
                string input = Console.ReadLine();
                if (input?.ToLower() == "sair") break;

                produto.Name = input;

                Console.Write("Digite o valor do produto (ou 'sair'): ");
                input = Console.ReadLine();
                if (input?.ToLower() == "sair") break;

                produto.Price = Double.Parse(input);

                var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = JsonSerializer.Serialize(produto) });

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
                    var produto = JsonSerializer.Deserialize<Product>(cr.Message.Value);
                    Console.WriteLine($"[✔] Mensagem recebida: {produto.Name + " " + produto.Price.ToString("N2")}");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}
