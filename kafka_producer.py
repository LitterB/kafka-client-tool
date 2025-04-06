from kafka import KafkaProducer
import json

class KafkaProducerClient:
    """
    Kafka生产者客户端，用于连接Kafka服务器并发送消息
    """
    
    def __init__(self, bootstrap_servers):
        """
        初始化Kafka生产者客户端
        
        Args:
            bootstrap_servers (str): Kafka服务器地址，格式为'host:port'，多个服务器用逗号分隔
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else str(v).encode('utf-8'),
            api_version=(0, 10, 2)  # 根据实际Kafka版本调整
        )
    
    def send_message(self, topic, message):
        """
        发送消息到指定的Topic
        
        Args:
            topic (str): 目标Topic名称
            message (str or dict): 要发送的消息，可以是字符串或字典
        
        Returns:
            Future: 消息发送的Future对象
        """
        future = self.producer.send(topic, message)
        # 确保消息被发送
        self.producer.flush()
        return future
    
    def close(self):
        """
        关闭生产者连接
        """
        if self.producer:
            self.producer.close()
            self.producer = None
    
    def __del__(self):
        """
        析构函数，确保资源被释放
        """
        self.close()