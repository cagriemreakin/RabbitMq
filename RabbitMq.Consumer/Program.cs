using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMq.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://vsezbtqj:Nf6BHHzO-RNCaW-rji1ewCnOTwRnup_7@grouse.rmq.cloudamqp.com/vsezbtqj");
            //factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    //channel.QueueDeclare("task_queue", durable:true,exclusive: false,autoDelete: false, null);
                    channel.ExchangeDeclare("logs", durable: true, type: ExchangeType.Fanout);
                    var queueName = channel.QueueDeclare().QueueName;

                    channel.QueueBind(queue:queueName, exchange:"logs", routingKey:"");

                    channel.BasicQos(prefetchSize:0,prefetchCount:1,global:false);

                    Console.WriteLine("Waiting logs");

                    var consumer = new EventingBasicConsumer(channel);

                    channel.BasicConsume(queue:queueName,autoAck:false,consumer);

                    consumer.Received += (model, ea) => {

                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine("Log alındı {0}", message);

                        int time = int.Parse(GetMessage(args));
                        Thread.Sleep(time);
                        Console.WriteLine("Log bitti.");
                        channel.BasicAck(ea.DeliveryTag,multiple:false);
                    };
                    Console.WriteLine("Consumer to exit!");
                    Console.ReadLine();
                }
            }
        }
        private static string GetMessage(string[] args)
        {
            return args[0].ToString();
        }
    }
}
