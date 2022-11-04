package topic

import (
	"awesomeProject/common/mqUtils"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerInfo struct {
	BindingKey string
	QueueName  string
}

func InitConsumer(exchangeType, exchangeName, consumerName string, queueName string, bindingKeys []string) {
	//1. 建立mq链接
	url := fmt.Sprintf("amqp://%s:%s@%s:5672/", mqUtils.MQ_USER, mqUtils.MQ_PWD, mqUtils.MQ_ADDR)
	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Println("mq connection error: ", err)
		return
	}

	//2. 创建信道
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("channel creation error: ", err)
		return
	}

	//3. 声明交换机
	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println("exchange declaration error: ", err)
		return
	}

	q := CreateQueue(ch, queueName)
	if q == nil {
		fmt.Println("create queue failure")
		return
	}
	for _, key := range bindingKeys {
		QueueBind(ch, exchangeName, queueName, key)
	}

	msgs, err := ch.Consume(
		q.Name,
		consumerName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("consume error: ", err)
		return
	}

	forever := make(chan interface{})
	go func() {
		for msg := range msgs {
			fmt.Println(consumerName, "receive ---> ", string(msg.Body), msg.RoutingKey)
		}

	}()
	<-forever
}

func CreateQueue(ch *amqp.Channel, queueName string) *amqp.Queue {
	if ch == nil {
		fmt.Println("channel is nil")
		return nil
	}

	//1. 声明临时队列
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println("temporary queue declaration error: ", err)
		return nil
	}

	return &q
}

func QueueBind(ch *amqp.Channel, exchangeName, queueName, bindingKey string) {
	//2. 队列和交换机绑定
	err := ch.QueueBind(
		queueName,
		bindingKey,
		exchangeName,
		false,
		nil)
	if err != nil {
		fmt.Println("queue binding error: ", err)
		return
	}
}
