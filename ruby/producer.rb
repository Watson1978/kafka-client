require "kafka"

kafka = Kafka.new(["localhost:9092"], client_id: "ruby-producer")
kafka.deliver_message("Hello, World!", topic: "test")
