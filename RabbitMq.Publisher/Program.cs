﻿using RabbitMQ.Client;
using System;
using System.Text;

namespace RabbitMq.Publisher
{
    public enum LogTypes
    {
        Critical=1,
        Error=2,
        Info=3,
        Warning=4
    }
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://vsezbtqj:Nf6BHHzO-RNCaW-rji1ewCnOTwRnup_7@grouse.rmq.cloudamqp.com/vsezbtqj");
            //factory.HostName = "localhost";

            // Eğer objemiz Idisposable ise işi bittikten sonra memoryden silinir.
            // Bunun için using kullanılır
            //IConnection : INetworkConnection, IDisposable
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    //durable true olursa rabbit mq onu fiziksel bir diske yazar ve restart olduğunda veriler kaybolmaz. False olduğunda memory de durur
                    //exclusive kuyruğa bir tane mi kanal bağlansın başka kanallarda bağlanabilrsin mi? False olursa diğerleride bağlanabilir.
                    //autoDelete kuyruktaki işlemler bittiğinde kuyruk silinsin mi?

                    //channel.QueueDeclare("task_queue",durable:true,false,false,null);
                    channel.ExchangeDeclare("direct_exchange",durable:true,type:ExchangeType.Direct);

                    Array log_name_array = Enum.GetValues(typeof(LogTypes));

                    for (int i =1; i < 11; i++)
                    {
                        Random rnd = new Random();
                        LogTypes log = (LogTypes) log_name_array.GetValue(rnd.Next(log_name_array.Length));
                        byte[] body = Encoding.UTF8.GetBytes($"log-{log.ToString()}");
                        
                        //mesajların silinmememsini sağlıyor
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;

                        //default exchange kullandığımız için ilkm parametre boş oldu
                        channel.BasicPublish(exchange: "direct_exchange", routingKey:log.ToString(), properties, body);
                        Console.WriteLine($"log-gonderildi.{log.ToString()}");
                    }
                   

                }
                Console.WriteLine("Click to exit!");
            }

        }

        private static string GetMessage(string[] args)
        {
            return args[0].ToString();
        }
    }
}
