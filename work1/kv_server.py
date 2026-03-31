import socket
import threading

class KVServer:
    def __init__(self, host='0.0.0.0', port=8888):
        self.host = host
        self.port = port
        self.store = {}  # 内存中的 Key-Value 存储
        self.lock = threading.Lock()  # 线程锁，保证并发下的数据安全
        
        # 初始化 TCP Socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100) # 设置最大挂起连接数

    def handle_client(self, conn, addr):
        """处理单个客户端的请求"""
        # print(f"[新连接] 客户端 {addr} 已连接。")
        with conn:
            while True:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    # 解码并去除两端的空白字符（包括换行符）
                    request = data.decode('utf-8').strip()
                    if not request:
                        continue

                    # 处理指令并获取返回结果
                    response = self.process_command(request)
                    
                    # 将结果发送回客户端，末尾加上 \n 以便 JMeter 等工具识别响应结束
                    conn.sendall((response + '\n').encode('utf-8'))

                    if response == "BYE":
                        break
                        
                except ConnectionResetError:
                    break
                except Exception as e:
                    print(f"[异常] 处理客户端 {addr} 时出错: {e}")
                    break
        # print(f"[断开连接] 客户端 {addr} 已断开。")

    def process_command(self, request):
        """解析并执行命令"""
        parts = request.split()
        if not parts:
            return "ERROR: Empty command"
            
        cmd = parts[0].upper()

        if cmd == 'PUT' and len(parts) >= 3:
            key = parts[1]
            value = ' '.join(parts[2:]) # 支持 value 中带有空格
            with self.lock:
                self.store[key] = value
            return "OK"
            
        elif cmd == 'GET' and len(parts) == 2:
            key = parts[1]
            with self.lock:
                value = self.store.get(key)
            return value if value is not None else "NOT_FOUND"
            
        elif cmd == 'DELETE' and len(parts) == 2:
            key = parts[1]
            with self.lock:
                if key in self.store:
                    del self.store[key]
                    return "OK"
                else:
                    return "NOT_FOUND"
                    
        elif cmd == 'EXIT':
            return "BYE"
            
        else:
            return "ERROR: Invalid command format"

    def start(self):
        """启动服务器"""
        print(f"Key-Value 服务器正在运行，监听 {self.host}:{self.port} ...")
        try:
            while True:
                conn, addr = self.server_socket.accept()
                # 每接收一个新连接，创建一个新线程来处理
                client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                client_thread.daemon = True # 设置为守护线程
                client_thread.start()
        except KeyboardInterrupt:
            print("\n正在关闭服务器...")
        finally:
            self.server_socket.close()

if __name__ == "__main__":
    server = KVServer()
    server.start()