import os
import shutil
import subprocess

def build_app():
    print("开始打包Kafka客户端工具...")
    
    # 确保assets目录存在
    if not os.path.exists('assets/icons'):
        os.makedirs('assets/icons', exist_ok=True)
    
    # 检查图标文件是否存在
    icon_path = 'assets/icons/kafka_icon.ico'
    if not os.path.exists(icon_path):
        print(f"警告: 图标文件 {icon_path} 不存在，将使用默认图标")
    
    # 清理之前的构建和缓存
    print("清理构建目录和缓存...")
    if os.path.exists('build'):
        shutil.rmtree('build')
    if os.path.exists('dist'):
        shutil.rmtree('dist')
    
    # 清理所有__pycache__文件夹
    for root, dirs, files in os.walk('.'):
        if '__pycache__' in dirs:
            pycache_path = os.path.join(root, '__pycache__')
            print(f"删除缓存: {pycache_path}")
            shutil.rmtree(pycache_path)
    
    # 使用PyInstaller打包
    try:
        subprocess.run(['pyinstaller', 'kafka_tool.spec'], check=True)
        print("打包完成！")
        print(f"可执行文件位于: {os.path.abspath('dist/Kafka客户端工具/Kafka客户端工具.exe')}")
    except subprocess.CalledProcessError as e:
        print(f"打包失败: {e}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    build_app()