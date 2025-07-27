from threading import Thread
from threading import Lock
import websocket
import json
import time
import ssl
import wave
import _thread as thread
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

from core import wsa_server
from scheduler.thread_manager import MyThread
from utils import util
from utils import config_util as cfg
from core.authorize_tb import Authorize_Tb

__running = True
__my_thread = None
_token = ''


def __post_token():
    global _token
    __client = AcsClient(
        cfg.key_ali_nls_key_id,
        cfg.key_ali_nls_key_secret,
        "cn-shanghai"
    )

    __request = CommonRequest()
    __request.set_method('POST')
    __request.set_domain('nls-meta.cn-shanghai.aliyuncs.com')
    __request.set_version('2019-02-28')
    __request.set_action_name('CreateToken')
    info = json.loads(__client.do_action_with_exception(__request))
    _token = info['Token']['Id']
    authorize = Authorize_Tb()
    authorize_info = authorize.find_by_userid(cfg.key_ali_nls_key_id)
    if authorize_info is not None:
        authorize.update_by_userid(
            cfg.key_ali_nls_key_id, _token, info['Token']['ExpireTime']*1000)
    else:
        authorize.add(cfg.key_ali_nls_key_id, _token,
                      info['Token']['ExpireTime']*1000)


def __runnable():
    while __running:
        __post_token()
        time.sleep(60 * 60 * 12)


def start():
    MyThread(target=__runnable).start()


class ALiNls:
    # 初始化
    def __init__(self, username):
        self.__URL = 'wss://nls-gateway-cn-shenzhen.aliyuncs.com/ws/v1'
        self.__ws = None
        self.__frames = []
        self.started = False
        self.__closing = False
        self.__task_id = ''
        self.done = False
        self.finalResults = ""
        self.username = username
        self.data = b''
        self.__endding = False
        self.__is_close = False
        self.lock = Lock()
        self.result_callback = None  # 添加回调函数
        print("aliyun asr created")

    def set_result_callback(self, callback):
        """设置结果回调函数"""
        self.result_callback = callback

    def __create_header(self, name):
        if name == 'StartTranscription':
            self.__task_id = util.random_hex(32)
        header = {
            "appkey": cfg.key_ali_nls_app_key,
            "message_id": util.random_hex(32),
            "task_id": self.__task_id,
            "namespace": "SpeechTranscriber",
            "name": name
        }
        return header

    # 收到websocket消息的处理
    def on_message(self, ws, message):
        try:
            print(f"[ALiNls-{self.username}] 收到阿里云消息: {message}")
            data = json.loads(message)
            header = data['header']
            name = header['name']

            if name == 'TranscriptionStarted':
                self.started = True
                print(f"[ALiNls-{self.username}] 转录已启动")

            elif name == 'SentenceEnd':
                self.done = True
                self.finalResults = data['payload']['result']
                print(f"[ALiNls-{self.username}] 最终结果: {self.finalResults}")

                # 调用回调函数通知ASR服务器
                if self.result_callback:
                    self.result_callback(self.finalResults, True)

                # 保持原有的wsa_server通知逻辑
                if wsa_server.get_web_instance().is_connected(self.username):
                    wsa_server.get_web_instance().add_cmd(
                        {"panelMsg": self.finalResults, "Username": self.username})
                if wsa_server.get_instance().is_connected(self.username):
                    content = {'Topic': 'human', 'Data': {
                        'Key': 'log', 'Value': self.finalResults}, 'Username': self.username}
                    wsa_server.get_instance().add_cmd(content)
                ws.close()

            elif name == 'TranscriptionResultChanged':
                self.finalResults = data['payload']['result']
                print(f"[ALiNls-{self.username}] 中间结果: {self.finalResults}")

                # 调用回调函数通知中间结果
                if self.result_callback:
                    self.result_callback(self.finalResults, False)

                # 保持原有的wsa_server通知逻辑
                if wsa_server.get_web_instance().is_connected(self.username):
                    wsa_server.get_web_instance().add_cmd(
                        {"panelMsg": self.finalResults, "Username": self.username})
                if wsa_server.get_instance().is_connected(self.username):
                    content = {'Topic': 'human', 'Data': {
                        'Key': 'log', 'Value': self.finalResults}, 'Username': self.username}
                    wsa_server.get_instance().add_cmd(content)

            elif name == 'TaskFailed':
                error_msg = header.get('status_text', 'Unknown error')
                print(f"[ALiNls-{self.username}] 任务失败: {error_msg}")

        except Exception as e:
            print(f"[ALiNls-{self.username}] 处理消息时出错: {e}")

    # 收到websocket的关闭要求
    def on_close(self, ws, code, msg):
        self.__endding = True
        self.__is_close = True
        print(
            f"[ALiNls-{self.username}] WebSocket连接已关闭 - 代码: {code}, 消息: {msg}")

    # 收到websocket错误的处理
    def on_error(self, ws, error):
        print(f"[ALiNls-{self.username}] WebSocket错误: {error}")
        self.started = True  # 避免在aliyun asr出错时，recorder一直等待start状态返回

    # 收到websocket连接建立的处理
    def on_open(self, ws):
        self.__endding = False
        print(f"[ALiNls-{self.username}] WebSocket连接已建立")

        def run(*args):
            sent_packets = 0
            sent_bytes = 0
            last_log_time = time.time()

            while self.__endding == False:
                try:
                    current_time = time.time()

                    if len(self.__frames) > 0:
                        with self.lock:
                            frame = self.__frames.pop(0)

                        if isinstance(frame, dict):
                            ws.send(json.dumps(frame))
                            print(
                                f"[ALiNls-{self.username}] 发送控制消息: {frame.get('header', {}).get('name', 'Unknown')}")
                        elif isinstance(frame, bytes):
                            ws.send(frame, websocket.ABNF.OPCODE_BINARY)
                            self.data += frame
                            sent_packets += 1
                            sent_bytes += len(frame)

                            # 每5秒或每100个包输出一次统计
                            if (sent_packets % 100 == 0 or
                                    current_time - last_log_time >= 5):
                                print(f"[ALiNls-{self.username}] 已发送音频 - 包数: {sent_packets}, "
                                      f"字节数: {sent_bytes}, 队列剩余: {len(self.__frames)}")
                                last_log_time = current_time
                    else:
                        time.sleep(0.001)  # 避免忙等
                except Exception as e:
                    print(f"[ALiNls-{self.username}] 发送数据时出错: {e}")
                    break

            print(
                f"[ALiNls-{self.username}] 发送线程结束 - 总发送: {sent_packets}包, {sent_bytes}字节")

            # 发送剩余数据和停止命令
            if self.__is_close == False:
                remaining_frames = len(self.__frames)
                if remaining_frames > 0:
                    print(
                        f"[ALiNls-{self.username}] 发送剩余 {remaining_frames} 个音频帧")
                    for frame in self.__frames:
                        ws.send(frame, websocket.ABNF.OPCODE_BINARY)

                frame = {"header": self.__create_header('StopTranscription')}
                ws.send(json.dumps(frame))
                print(f"[ALiNls-{self.username}] 发送停止转录命令")

        thread.start_new_thread(run, ())

    def __connect(self):
        self.finalResults = ""
        self.done = False
        with self.lock:
            self.__frames.clear()
        self.__ws = websocket.WebSocketApp(
            self.__URL + '?token=' + _token, on_message=self.on_message)
        self.__ws.on_open = self.on_open
        self.__ws.on_error = self.on_error
        self.__ws.on_close = self.on_close
        self.__ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def send(self, buf):
        """发送音频数据到阿里云"""
        with self.lock:
            self.__frames.append(buf)
            # 添加音频数据接收日志
            if isinstance(buf, bytes):
                print(
                    f"[ALiNls-{self.username}] 接收音频数据: {len(buf)} bytes, 队列长度: {len(self.__frames)}")

    def start(self):
        print(f"[ALiNls-{self.username}] 启动ASR连接")
        Thread(target=self.__connect, args=[]).start()
        data = {
            'header': self.__create_header('StartTranscription'),
            "payload": {
                "format": "pcm",
                "sample_rate": 16000,
                "enable_intermediate_result": True,
                "enable_punctuation_prediction": False,
                "enable_inverse_text_normalization": True,
                "speech_noise_threshold": -1
            }
        }
        self.send(data)
        print(f"[ALiNls-{self.username}] 发送启动转录命令")

    def end(self):
        print(f"[ALiNls-{self.username}] 结束ASR会话")
        self.__endding = True

        # 保存音频数据
        if len(self.data) > 0:
            with wave.open('cache_data/input2.wav', 'wb') as wf:
                n_channels = 1
                sampwidth = 2
                wf.setnchannels(n_channels)
                wf.setsampwidth(sampwidth)
                wf.setframerate(16000)
                wf.writeframes(self.data)
            print(f"[ALiNls-{self.username}] 音频数据已保存: {len(self.data)} bytes")

        self.data = b''
