# Kafka客户端工具

一个基于PySide6的Kafka客户端工具，提供了直观的图形界面来管理和监控Kafka主题、生产者和消费者。

## 功能特点

- 连接到Kafka服务器
- 查看和管理Kafka主题
- 发送消息到指定主题
- 消费来自指定主题的消息
- 实时监控消息流

## 安装方法

### 方法1: 下载可执行文件

从[Releases](https://github.com/LitterB/kafka-client-tool/releases)页面下载最新版本的可执行文件。

### 方法2: 从源码安装

```bash
# 克隆仓库
git clone https://github.com/LitterB/kafka-client-tool.git
cd kafka-client-tool

# 安装依赖
pip install -r requirements.txt

# 运行应用
python main.py