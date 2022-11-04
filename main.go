package main

import (
	"awesomeProject/04-exchange/fanout"
	"awesomeProject/05-topic/topic"
	"fmt"
)

func main() {
	fmt.Println("Press 1 to start publisher, 2 to start consumer1, 3 to start consumer2")
	var startType string
	fmt.Scanf("%s", &startType)
	exchangeType := "topic"
	exchangeName := "topic_exchange"
	if startType == "1" {
		ch := fanout.InitPublisher(exchangeType, exchangeName)
		if ch == nil {
			return
		}

		forever := make(chan interface{})
		go func() {
			fanout.PublishMessage("send to q1 and q2", ch, exchangeName, "quick.orange.rabbit")
			fanout.PublishMessage("send to q1", ch, exchangeName, "qlazy.orange.elephant")
			fanout.PublishMessage("send to q2", ch, exchangeName, "lazy.brown.fox")
			fanout.PublishMessage("send to q2", ch, exchangeName, "lazy.pink.rabbit")
			fanout.PublishMessage("send to no one", ch, exchangeName, "quick.brown.fox")
			fanout.PublishMessage("send to q1", ch, exchangeName, "quick.orange.fox")
		}()
		<-forever
	} else if startType == "2" {
		forever := make(chan interface{})
		go topic.InitConsumer(exchangeType,
			exchangeName,
			"c1",
			"Q1",
			[]string{"*.orange.*"})
		<-forever
	} else {
		forever := make(chan interface{})
		go topic.InitConsumer(exchangeType,
			exchangeName,
			"c2",
			"Q2",
			[]string{"*.*.rabbit", "lazy.#"})
		<-forever
	}
}
