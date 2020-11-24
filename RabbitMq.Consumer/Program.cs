using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.IO;
using System.Text;
using System.Threading;

namespace RabbitMq.Consumer
{
    public enum LogTypes
    {
        Critical = 1,
        Error = 2,
        //Info = 3,
        //Warning = 4
    }
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
                    channel.ExchangeDeclare("topic_exchange", durable: true, type: ExchangeType.Topic);

                    var queueName = channel.QueueDeclare().QueueName;

                    string routingKey = "#.Warning";
                      channel.QueueBind(queue:queueName, exchange: "topic_exchange", routingKey:routingKey);


                    channel.BasicQos(prefetchSize:0,prefetchCount:1,global:false);

                    Console.WriteLine("Waiting logs");

                    var consumer = new EventingBasicConsumer(channel);

                    channel.BasicConsume(queue:queueName,autoAck:false,consumer);

                    consumer.Received += (model, ea) => {

                        var log = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"Log alındı {log}");

                        int time = int.Parse(GetMessage(args));
                        Thread.Sleep(time);

                        File.AppendAllText("logs_critical_error", log + "\n");

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
