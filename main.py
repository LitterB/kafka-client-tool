import sys
import os
import threading
import time
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                              QLabel, QLineEdit, QPushButton, QTextEdit, QComboBox, QListWidget,
                              QGroupBox, QSplitter, QTabWidget, QGridLayout, QFrame, QStyleFactory)
from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtGui import QTextCursor, QColor, QPalette, QIcon

from kafka_producer import KafkaProducerClient
from kafka_consumer import KafkaConsumerClient

class KafkaClientApp(QMainWindow):
    # 信号定义
    log_signal = Signal(str)
    received_message_signal = Signal(str, str)
    topics_list_signal = Signal(list)
    
    # 在__init__方法中修改设置图标的代码
    def __init__(self):
        super().__init__()
        self.kafka_client = None
        self.producer_client = None
        self.consumer_client = None
        self.consumer_threads = {}
        
        self.setWindowTitle("Kafka客户端工具")
        self.resize(1200, 800)

         # 设置应用图标
        # 修改图标路径处理方式，使其在打包后也能正确找到
        import sys
        if getattr(sys, 'frozen', False):
            # 如果是打包后的应用
            application_path = sys._MEIPASS
            icon_path = os.path.join(application_path, "assets", "icons", "kafka_icon.ico")
        else:
            # 如果是直接运行的脚本
            icon_path = "./assets/icons/kafka_icon.ico"
        
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        else:
            print(f"图标文件不存在: {icon_path}")
    
        
        # 设置主布局
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        main_layout = QVBoxLayout(self.central_widget)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(8)
        
        # 改进样式表，使用自定义样式
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 5px;
                margin-top: 0.5em;
                padding-top: 0.5em;
                background-color: #ffffff;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 5px;
                left: 10px;
                color: #2c3e50;
            }
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 5px 10px;
                min-height: 25px;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #1c6ea4;
            }
            QPushButton:disabled {
                background-color: #d0d0d0;
                color: #a0a0a0;
            }
            QLineEdit {
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                padding: 3px;
                background-color: #ffffff;
            }
            QTextEdit {
                font-family: "Consolas", "Courier New", monospace;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #ffffff;
            }
            QComboBox {
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                padding: 3px;
                padding-right: 20px;
                background-color: #ffffff;
                min-height: 25px;
            }
            QComboBox QAbstractItemView {
                selection-background-color: #3498db;
                selection-color: #3498db;
                background-color: white;
                outline: 0px;
            }
            QComboBox:hover {
                border: 1px solid #3498db;
            }
            QListWidget {
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #ffffff;
            }
            QListWidget::item:selected {
                background-color: #3498db;
                color: white;
            }
            QLabel {
                color: #2c3e50;
            }
        """)
        
        # 创建连接配置区域
        self.create_connection_section()
        main_layout.addWidget(self.connection_widget)
        
        # 创建水平分割器
        self.splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(self.splitter)
        
        # 创建生产者和消费者区域
        self.create_producer_section()
        self.create_consumer_section()
        
        # 添加到分割器
        self.splitter.addWidget(self.producer_widget)
        self.splitter.addWidget(self.consumer_widget)
        self.splitter.setSizes([600, 600])
        
        # 创建日志区域
        self.create_log_section()
        main_layout.addWidget(self.log_widget)
        
        # 设置主布局的比例 - 让中间部分占据更多空间
        main_layout.setStretch(0, 0)  # 连接区域
        main_layout.setStretch(1, 4)  # 中间的生产者/消费者区域
        main_layout.setStretch(2, 1)  # 日志区域
        
        # 连接信号
        self.log_signal.connect(self.update_log)
        self.received_message_signal.connect(self.update_received_messages)
        self.topics_list_signal.connect(self.update_topics_list)
    
    def create_connection_section(self):
        self.connection_widget = QGroupBox("Kafka连接配置")
        layout = QHBoxLayout(self.connection_widget)
        layout.setContentsMargins(8, 15, 8, 8)
        
        # 使用水平布局，按照指定比例分配空间
        # 主机和端口区域 - 占1/2
        host_port_widget = QWidget()
        host_port_layout = QHBoxLayout(host_port_widget)
        host_port_layout.setContentsMargins(0, 0, 0, 0)
        
        # 主机输入
        host_layout = QHBoxLayout()
        host_layout.addWidget(QLabel("主机:"))
        self.host = QLineEdit("localhost")
        self.host.setPlaceholderText("输入Kafka主机地址")
        host_layout.addWidget(self.host)
        host_port_layout.addLayout(host_layout, 4)  # 主机占更多空间
        
        # 端口输入
        port_layout = QHBoxLayout()
        port_layout.addWidget(QLabel("端口:"))
        self.port = QLineEdit("9092")
        self.port.setPlaceholderText("端口")
        self.port.setMaximumWidth(70)  # 限制端口输入框宽度
        port_layout.addWidget(self.port)
        host_port_layout.addLayout(port_layout, 1)  # 端口占较少空间
        
        # 连接按钮、断开连接和状态区域 - 占3/8
        conn_status_widget = QWidget()
        conn_status_layout = QHBoxLayout(conn_status_widget)
        conn_status_layout.setContentsMargins(0, 0, 0, 0)
        
        # 连接和断开按钮
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(5)
        
        self.connect_btn = QPushButton("连接")
        self.connect_btn.clicked.connect(self.connect_kafka)
        self.connect_btn.setMinimumWidth(70)
        buttons_layout.addWidget(self.connect_btn)
        
        self.disconnect_btn = QPushButton("断开连接")
        self.disconnect_btn.clicked.connect(self.disconnect_kafka)
        self.disconnect_btn.setEnabled(False)
        self.disconnect_btn.setMinimumWidth(70)
        buttons_layout.addWidget(self.disconnect_btn)
        
        conn_status_layout.addLayout(buttons_layout)
        
        # 连接状态
        status_layout = QHBoxLayout()
        status_layout.addWidget(QLabel("连接状态:"))
        self.connection_status = QLabel("未连接")
        self.connection_status.setStyleSheet("color: red; font-weight: bold;")
        status_layout.addWidget(self.connection_status)
        
        conn_status_layout.addLayout(status_layout)
        
        # 样式选择区域 - 占1/8
        theme_widget = QWidget()
        theme_layout = QHBoxLayout(theme_widget)
        theme_layout.setContentsMargins(0, 0, 0, 0)
        
        theme_layout.addWidget(QLabel("样式:"))
        self.style_selector = QComboBox()
        # 获取系统可用的样式
        self.style_selector.addItems(QStyleFactory.keys())
        # 设置默认样式为Fusion (跨平台且美观)
        self.style_selector.setCurrentText("Fusion")
        self.style_selector.currentTextChanged.connect(self.change_style)
        theme_layout.addWidget(self.style_selector)
        
        # 按照指定比例添加到主布局
        layout.addWidget(host_port_widget, 4)       # 主机和端口占1/2
        layout.addWidget(conn_status_widget, 3)     # 连接按钮、断开连接和状态占3/8
        layout.addWidget(theme_widget, 1)           # 样式选择占1/8
        
        # 设置合理的高度
        self.connection_widget.setMinimumHeight(80)
        self.connection_widget.setMaximumHeight(100)
    
    def create_producer_section(self):
        self.producer_widget = QGroupBox("生产者")
        layout = QVBoxLayout(self.producer_widget)
        layout.setContentsMargins(8, 15, 8, 8)
        
        # 消息发送区域
        message_group = QGroupBox("消息发送")
        message_layout = QVBoxLayout(message_group)
        
        # Topic输入框
        topic_layout = QHBoxLayout()
        topic_layout.addWidget(QLabel("Topic:"))
        self.producer_topic = QLineEdit()
        self.producer_topic.setPlaceholderText("输入目标Topic")
        topic_layout.addWidget(self.producer_topic)
        message_layout.addLayout(topic_layout)
        
        # 消息输入区域
        message_layout.addWidget(QLabel("消息:"))
        self.producer_message = QTextEdit()
        self.producer_message.setPlaceholderText("输入要发送的消息内容")
        self.producer_message.setMinimumHeight(80)
        message_layout.addWidget(self.producer_message)
        
        # 发送按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.send_message_btn = QPushButton("发送消息")
        self.send_message_btn.clicked.connect(self.send_message)
        self.send_message_btn.setEnabled(False)
        button_layout.addWidget(self.send_message_btn)
        message_layout.addLayout(button_layout)
        
        layout.addWidget(message_group)
        
        # 生产者日志区域 - 增加高度
        producer_log_group = QGroupBox("生产者日志")
        producer_log_layout = QVBoxLayout(producer_log_group)
        
        self.producer_log = QTextEdit()
        self.producer_log.setReadOnly(True)
        producer_log_layout.addWidget(self.producer_log)
        
        layout.addWidget(producer_log_group)
        
        # 设置比例 - 让日志区域占更多空间
        layout.setStretch(0, 1)  # 消息发送区域
        layout.setStretch(1, 3)  # 生产者日志区域
    
    def create_consumer_section(self):
        self.consumer_widget = QGroupBox("消费者")
        layout = QVBoxLayout(self.consumer_widget)
        layout.setContentsMargins(8, 15, 8, 8)
        
        # 添加消费者组ID到消费者区域
        group_id_layout = QHBoxLayout()
        group_id_layout.addWidget(QLabel("消费者组ID:"))
        self.group_id = QLineEdit("my-group")
        self.group_id.setPlaceholderText("输入消费者组ID")
        group_id_layout.addWidget(self.group_id)
        layout.addLayout(group_id_layout)
        
        # Topic订阅区域
        topic_group = QGroupBox("Topic订阅")
        topic_layout = QVBoxLayout(topic_group)
        
        # 可用Topics区域
        available_layout = QHBoxLayout()
        available_layout.addWidget(QLabel("可用Topics:"))
        self.available_topics = QComboBox()
        self.available_topics.currentTextChanged.connect(self.on_available_topic_changed)
        available_layout.addWidget(self.available_topics)
        
        # 添加刷新按钮
        self.refresh_topics_btn = QPushButton("刷新")
        self.refresh_topics_btn.clicked.connect(self.refresh_topics)
        self.refresh_topics_btn.setEnabled(False)
        available_layout.addWidget(self.refresh_topics_btn)
        
        topic_layout.addLayout(available_layout)
        
        # 订阅Topic输入框
        topic_input_layout = QHBoxLayout()
        topic_input_layout.addWidget(QLabel("Topic:"))
        self.manual_topic = QLineEdit()
        self.manual_topic.setPlaceholderText("输入要订阅的Topic")
        topic_input_layout.addWidget(self.manual_topic)
        
        # 统一的订阅按钮
        self.subscribe_btn = QPushButton("订阅")
        self.subscribe_btn.clicked.connect(self.subscribe_topic_from_input)
        self.subscribe_btn.setEnabled(False)
        topic_input_layout.addWidget(self.subscribe_btn)
        
        topic_layout.addLayout(topic_input_layout)
        
        layout.addWidget(topic_group)
        
        # 已订阅Topics列表
        subscribed_group = QGroupBox("已订阅Topics")
        subscribed_layout = QVBoxLayout(subscribed_group)
        
        self.subscribed_topics = QListWidget()
        self.subscribed_topics.itemClicked.connect(self.on_topic_selected)
        subscribed_layout.addWidget(self.subscribed_topics)
        
        selected_layout = QHBoxLayout()
        selected_layout.addWidget(QLabel("选中的Topic:"))
        self.selected_topic = QLineEdit()
        self.selected_topic.setReadOnly(True)
        selected_layout.addWidget(self.selected_topic)
        
        self.unsubscribe_btn = QPushButton("取消订阅")
        self.unsubscribe_btn.clicked.connect(self.unsubscribe_topic)
        selected_layout.addWidget(self.unsubscribe_btn)
        
        subscribed_layout.addLayout(selected_layout)
        
        layout.addWidget(subscribed_group)
        
        # 接收消息区域 - 增加高度
        received_group = QGroupBox("接收到的消息")
        received_layout = QVBoxLayout(received_group)
        
        self.received_messages = QTextEdit()
        self.received_messages.setReadOnly(True)
        received_layout.addWidget(self.received_messages)
        
        layout.addWidget(received_group)
        
        # 设置比例 - 让消息区域占更多空间
        layout.setStretch(0, 0)  # 消费者组ID
        layout.setStretch(1, 1)  # Topic订阅
        layout.setStretch(2, 1)  # 已订阅Topics
        layout.setStretch(3, 4)  # 接收消息区域

    # 当选择可用Topic时，自动填充到输入框
    def on_available_topic_changed(self, text):
        if text:
            self.manual_topic.setText(text)
    
    # 从输入框订阅Topic
    def subscribe_topic_from_input(self):
        topic = self.manual_topic.text()
        if not topic:
            self.log_signal.emit("请输入要订阅的Topic")
            return
        
        self.subscribe_topic(topic)
    
    # 刷新可用Topics列表
    def refresh_topics(self):
        if not hasattr(self, 'bootstrap_servers'):
            self.log_signal.emit("请先连接到Kafka服务器")
            return
            
        try:
            # 创建临时的管理客户端来获取topics列表
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='kafka-tool-admin-temp'
            )
            
            # 获取可用的topics列表
            topics = admin_client.list_topics()
            # 过滤掉内部Topic（以_开头的）
            topics = [topic for topic in topics if not topic.startswith('_')]
            self.topics_list_signal.emit(topics)
            
            # 关闭临时客户端
            admin_client.close()
            
            self.log_signal.emit("已刷新可用Topics列表")
        except Exception as e:
            self.log_signal.emit(f"刷新Topics列表失败: {str(e)}")
    
    def create_log_section(self):
        self.log_widget = QGroupBox("系统日志")
        layout = QVBoxLayout(self.log_widget)
        layout.setContentsMargins(8, 15, 8, 8)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(120)
        layout.addWidget(self.log_text)
    
    # Kafka连接相关方法
    def connect_kafka(self):
        host = self.host.text()
        port = self.port.text()
        bootstrap_servers = f"{host}:{port}"
        
        # 更新日志
        log_message = f"正在连接到Kafka服务器: {bootstrap_servers}..."
        self.log_signal.emit(log_message)
        
        try:
            # 创建临时的管理客户端来获取topics列表
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='kafka-tool-admin-temp'
            )
            
            # 获取可用的topics列表
            topics = admin_client.list_topics()
            # 过滤掉内部Topic（以_开头的）
            topics = [topic for topic in topics if not topic.startswith('_')]
            self.topics_list_signal.emit(topics)
            
            # 关闭临时客户端
            admin_client.close()
            
            # 更新UI状态
            self.connect_btn.setEnabled(False)
            self.disconnect_btn.setEnabled(True)
            self.send_message_btn.setEnabled(True)
            self.subscribe_btn.setEnabled(True)
            self.refresh_topics_btn.setEnabled(True)
            
            # 更新连接状态
            self.connection_status.setText("已连接")
            self.connection_status.setStyleSheet("color: green;")
            
            # 保存连接信息供后续使用
            self.bootstrap_servers = bootstrap_servers
            
            self.log_signal.emit("连接成功!")
        except Exception as e:
            self.log_signal.emit(f"连接失败: {str(e)}")
    
    # 生产者相关方法
    def send_message(self):
        topic = self.producer_topic.text()
        message = self.producer_message.toPlainText()
        
        if not topic or not message:
            self.log_signal.emit("Topic和消息不能为空")
            return
        
        try:
            # 如果生产者客户端不存在，创建一个
            if not self.producer_client:
                self.producer_client = KafkaProducerClient(self.bootstrap_servers)
            
            self.producer_client.send_message(topic, message)
            
            # 添加时间戳
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            producer_log = f"[{timestamp}] 消息已发送到Topic: {topic}\n内容: {message}"
            self.producer_log.append(producer_log)
            self.log_signal.emit(f"消息已发送到Topic: {topic}")
            # 清空消息输入框
            self.producer_message.clear()
        except Exception as e:
            self.log_signal.emit(f"发送失败: {str(e)}")
    
    # 消费者相关方法
    # 订阅选中的Topic
    def subscribe_selected_topic(self):
        topic = self.available_topics.currentText()
        if not topic:
            self.log_signal.emit("请选择一个Topic")
            return
        
        self.subscribe_topic(topic)
    
    # 订阅手动输入的Topic
    def subscribe_manual_topic(self):
        topic = self.manual_topic.text()
        if not topic:
            self.log_signal.emit("请输入要订阅的Topic")
            return
        
        self.subscribe_topic(topic)
    
    # 修改原有的subscribe_topic方法
    def subscribe_topic(self, topic):
        if not hasattr(self, 'bootstrap_servers'):
            self.log_signal.emit("请先连接到Kafka服务器")
            return
        
        group_id = self.group_id.text()
        
        if not group_id:
            self.log_signal.emit("请输入消费者组ID")
            return
        
        if topic in self.consumer_threads and self.consumer_threads[topic].is_alive():
            self.log_signal.emit(f"已经订阅了Topic: {topic}")
            return
        
        try:
            # 如果消费者客户端不存在，创建一个
            if not self.consumer_client:
                self.consumer_client = KafkaConsumerClient(self.bootstrap_servers, group_id)
            
            # 添加到已订阅列表
            self.subscribed_topics.addItem(topic)
            
            # 启动消费线程
            def consume_messages():
                try:
                    for message in self.consumer_client.consume_topic(topic):
                        # 使用信号更新UI
                        self.received_message_signal.emit(topic, message)
                        time.sleep(0.1)  # 避免UI卡顿
                except Exception as e:
                    self.log_signal.emit(f"消费失败: {str(e)}")
            
            consumer_thread = threading.Thread(target=consume_messages, daemon=True)
            consumer_thread.start()
            self.consumer_threads[topic] = consumer_thread
            
            self.log_signal.emit(f"已订阅Topic: {topic}")
        except Exception as e:
            self.log_signal.emit(f"订阅失败: {str(e)}")
    
    def unsubscribe_topic(self):
        selected_topic = self.selected_topic.text()
        if not selected_topic:
            self.log_signal.emit("请先选择要取消订阅的Topic")
            return
        
        self.unsubscribe_topic_internal(selected_topic)
    
    def unsubscribe_topic_internal(self, topic):
        try:
            # 从已订阅列表中移除
            items = self.subscribed_topics.findItems(topic, Qt.MatchExactly)
            if items:
                for item in items:
                    row = self.subscribed_topics.row(item)
                    self.subscribed_topics.takeItem(row)
            
            # 停止消费线程
            if topic in self.consumer_threads:
                self.consumer_client.stop_consuming(topic)
                del self.consumer_threads[topic]
            
            self.log_signal.emit(f"已取消订阅Topic: {topic}")
            self.selected_topic.clear()
        except Exception as e:
            self.log_signal.emit(f"取消订阅失败: {str(e)}")
    
    def on_topic_selected(self, item):
        self.selected_topic.setText(item.text())
    
    # 信号槽方法
    @Slot(str)
    def update_log(self, message):
        # 添加时间戳
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 确保消息正确换行显示
        formatted_message = f"[{timestamp}] {message}"
        
        # 添加消息并确保滚动到最新内容
        self.log_text.append(formatted_message)
        self.log_text.ensureCursorVisible()
        
        # 滚动到底部 - 修复End属性错误
        cursor = self.log_text.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_text.setTextCursor(cursor)
    
    @Slot(str, str)
    def update_received_messages(self, topic, message):
        # 修改消息显示格式，只展示接收到的内容
        # 从消息中提取实际内容
        content = message
        if "] Key:" in message and ", Value:" in message:
            # 提取Value部分
            content = message.split(", Value:")[1].strip()
        
        # 添加时间戳
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.received_messages.append(f"[{timestamp}] [{topic}] {content}")
    
    @Slot(list)
    def update_topics_list(self, topics):
        self.available_topics.clear()
        self.available_topics.addItems(topics)

    # Fix indentation - these methods should be at the class level, not nested
    def disconnect_kafka(self):
        try:
            # 关闭所有消费线程 - 修复迭代问题
            # 先创建一个副本，然后迭代副本
            topics_to_unsubscribe = list(self.consumer_threads.keys())
            for topic in topics_to_unsubscribe:
                self.unsubscribe_topic_internal(topic)
            
            # 确保消费者线程字典已清空
            self.consumer_threads.clear()
            
            # 关闭客户端连接
            if self.consumer_client:
                self.consumer_client.close()
                self.consumer_client = None
            
            if self.producer_client:
                self.producer_client = None
            
            # 更新UI状态 - 只引用存在的按钮
            self.connect_btn.setEnabled(True)
            self.disconnect_btn.setEnabled(False)
            self.send_message_btn.setEnabled(False)
            self.subscribe_btn.setEnabled(False)
            self.refresh_topics_btn.setEnabled(False)
            
            # 更新连接状态
            self.connection_status.setText("未连接")
            self.connection_status.setStyleSheet("color: red;")
            
            # 清空已订阅列表
            self.subscribed_topics.clear()
            self.selected_topic.clear()
            
            # 移除bootstrap_servers属性
            if hasattr(self, 'bootstrap_servers'):
                delattr(self, 'bootstrap_servers')
            
            self.log_signal.emit("已断开连接")
        except Exception as e:
            self.log_signal.emit(f"断开连接失败: {str(e)}")
            import traceback
            self.log_signal.emit(traceback.format_exc())

    def change_style(self, style_name):
        """切换应用样式"""
        try:
            # 应用选择的样式
            QApplication.setStyle(QStyleFactory.create(style_name))
            
            # 保持自定义样式表
            self.log_signal.emit(f"已切换到样式: {style_name}")
        except Exception as e:
            self.log_signal.emit(f"切换样式失败: {str(e)}")

    def change_theme(self, theme_name):
        """切换应用主题"""
        try:
            apply_stylesheet(QApplication.instance(), theme=f'{theme_name}.xml')
            self.log_signal.emit(f"已切换到主题: {theme_name}")
        except Exception as e:
            self.log_signal.emit(f"切换主题失败: {str(e)}")

if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        
        # 设置Fusion样式作为默认样式
        app.setStyle(QStyleFactory.create("Fusion"))
        
        # 创建并显示主窗口
        window = KafkaClientApp()
        window.show()
        
        sys.exit(app.exec())
    except Exception as e:
        print(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)