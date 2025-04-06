from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json
import threading

class KafkaConsumerClient:
    """
    Kafka消费者客户端，用于连接Kafka服务器、订阅Topic和消费消息
    """
    
    def __init__(self, bootstrap_servers, group_id):
        """
        初始化Kafka消费者客户端
        
        Args:
            bootstrap_servers (str): Kafka服务器地址，格式为'host:port'，多个服务器用逗号分隔
            group_id (str): 消费者组ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-tool-admin'
        )
        self._initialize()
    
    def list_topics(self):
        """
        获取Kafka服务器上可用的Topic列表
        
        Returns:
            list: Topic名称列表
        """
        try:
            topics = self.admin_client.list_topics()
            # 过滤掉内部Topic（以_开头的）
            return [topic for topic in topics if not topic.startswith('_')]
        except Exception as e:
            print(f"获取Topic列表失败: {str(e)}")
            return []
    
    def consume_topic(self, topic):
        """
        消费指定Topic的消息
        
        Args:
            topic (str): 要消费的Topic名称
        
        Yields:
            str: 消费到的消息
        """
        # 创建消费者
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='latest',  # 从最新的消息开始消费
            value_deserializer=lambda x: self._deserialize_message(x),
            api_version=(0, 10, 2)  # 根据实际Kafka版本调整
        )
        
        # 保存消费者实例
        self.consumers[topic] = consumer
        self.stop_flags[topic] = threading.Event()
        
        # 消费消息
        try:
            while not self.stop_flags[topic].is_set():
                # 非阻塞方式轮询消息，超时时间为100ms
                records = consumer.poll(timeout_ms=100, max_records=10)
                
                for tp, messages in records.items():
                    for message in messages:
                        # 格式化消息
                        formatted_message = self._format_message(message)
                        yield formatted_message
        except Exception as e:
            print(f"消费Topic {topic}失败: {str(e)}")
        finally:
            # 关闭消费者
            consumer.close()
            if topic in self.consumers:
                del self.consumers[topic]
            if topic in self.stop_flags:
                del self.stop_flags[topic]
    
    def stop_consuming(self, topic):
        """
        停止消费指定Topic
        
        Args:
            topic (str): 要停止消费的Topic名称
        """
        if topic in self.stop_flags:
            self.stop_flags[topic].set()
    
    def _deserialize_message(self, message_bytes):
        """
        反序列化消息
        
        Args:
            message_bytes (bytes): 消息字节
        
        Returns:
            str or dict: 反序列化后的消息
        """
        if not message_bytes:
            return ""
        
        try:
            # 尝试解析为JSON
            return json.loads(message_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 如果不是JSON，则尝试解析为字符串
            try:
                return message_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # 如果无法解码为UTF-8，则返回原始字节的字符串表示
                return str(message_bytes)
    
    def _format_message(self, message):
        """
        格式化消息，包含时间戳、键和值
        
        Args:
            message (ConsumerRecord): 消费者记录
        
        Returns:
            str: 格式化后的消息
        """
        timestamp = message.timestamp
        key = message.key.decode('utf-8') if message.key else 'None'
        value = message.value
        
        if isinstance(value, dict):
            value = json.dumps(value, ensure_ascii=False)
        
        return f"[{timestamp}] Key: {key}, Value: {value}"
    
    def close(self):
        """
        关闭所有消费者连接
        """
        # 创建一个字典副本进行迭代，避免在迭代过程中修改字典
        consumers_copy = list(self.consumers.values())
        for consumer in consumers_copy:
            try:
                consumer.close()
            except Exception as e:
                print(f"关闭消费者时出错: {str(e)}")
        
        # 清空字典
        self.consumers.clear()
        self.stop_flags.clear()
        # 停止所有消费线程
        for topic in list(self.stop_flags.keys()):
            self.stop_consuming(topic)
        
        if self.admin_client:
            self.admin_client.close()
    
    def __del__(self):
        self.close()

    def _initialize(self):
        """
        初始化Kafka消费者客户端
        """
        # 确保属性已初始化
        if not hasattr(self, 'consumers'):
            self.consumers = {}
        if not hasattr(self, 'stop_flags'):
            self.stop_flags = {}