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
    try:
        print(f"[Token] 开始获取阿里云NLS Token...")
        print(
            f"[Token] 使用配置 - KeyID: {cfg.key_ali_nls_key_id[:8]}..., KeySecret: {cfg.key_ali_nls_key_secret[:8]}...")

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

        print(f"[Token] 发送Token请求到阿里云...")
        info = json.loads(__client.do_action_with_exception(__request))
        _token = info['Token']['Id']

        print(f"[Token] Token获取成功: {_token[:20]}...")
        print(f"[Token] Token过期时间: {info['Token']['ExpireTime']}")

        authorize = Authorize_Tb()
        authorize_info = authorize.find_by_userid(cfg.key_ali_nls_key_id)
        if authorize_info is not None:
            authorize.update_by_userid(
                cfg.key_ali_nls_key_id, _token, info['Token']['ExpireTime']*1000)
        else:
            authorize.add(cfg.key_ali_nls_key_id, _token,
                          info['Token']['ExpireTime']*1000)
        print(f"[Token] Token已保存到数据库")

    except Exception as e:
        print(f"[Token] Token获取失败: {e}")
        print(f"[Token] 错误类型: {type(e).__name__}")
        import traceback
        traceback.print_exc()


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

        # 添加静音数据和时间跟踪
        self.__silence_data = bytes(32)  # 32字节的静音数据(1毫秒)
        self.__last_send_time = 0  # 最后发送数据的时间
        self.__keepalive_interval = 5.0  # 5秒无数据时发送静音包

    def set_result_callback(self, callback):
        """设置结果回调函数"""
        self.result_callback = callback

    def __create_header(self, name):
        if name == 'StartTranscription':
            self.__task_id = util.random_hex(32)
            print(f"[ALiNls-{self.username}] 生成新的TaskID: {self.__task_id}")

        header = {
            "appkey": cfg.key_ali_nls_app_key,
            "message_id": util.random_hex(32),
            "task_id": self.__task_id,
            "namespace": "SpeechTranscriber",
            "name": name
        }

        print(f"[ALiNls-{self.username}] 创建消息头: {header}")
        return header

    # 收到websocket消息的处理
    def on_message(self, ws, message):
        try:
            print(f"[ALiNls-{self.username}] ← 收到阿里云消息 (长度: {len(message)})")
            print(f"[ALiNls-{self.username}] 消息内容: {message}")

            data = json.loads(message)
            header = data.get('header', {})
            name = header.get('name', 'Unknown')

            print(f"[ALiNls-{self.username}] 消息类型: {name}")
            print(f"[ALiNls-{self.username}] 消息头: {header}")

            if name == 'TranscriptionStarted':
                self.started = True
                print(f"[ALiNls-{self.username}] ✓ 转录已启动")

            elif name == 'SentenceEnd':
                self.done = True
                self.finalResults = data.get('payload', {}).get('result', '')
                print(f"[ALiNls-{self.username}] ✓ 最终结果: {self.finalResults}")

                # 调用回调函数通知ASR服务器
                if self.result_callback:
                    print(f"[ALiNls-{self.username}] 调用结果回调函数 (final=True)")
                    self.result_callback(self.finalResults, True)

                # 保持原有的wsa_server通知逻辑
                if wsa_server.get_web_instance().is_connected(self.username):
                    wsa_server.get_web_instance().add_cmd(
                        {"panelMsg": self.finalResults, "Username": self.username})
                if wsa_server.get_instance().is_connected(self.username):
                    content = {'Topic': 'human', 'Data': {
                        'Key': 'log', 'Value': self.finalResults}, 'Username': self.username}
                    wsa_server.get_instance().add_cmd(content)

                print(f"[ALiNls-{self.username}] 准备关闭WebSocket连接")
                ws.close()

            elif name == 'TranscriptionResultChanged':
                self.finalResults = data.get('payload', {}).get('result', '')
                print(f"[ALiNls-{self.username}] ◐ 中间结果: {self.finalResults}")

                # 调用回调函数通知中间结果
                if self.result_callback:
                    print(f"[ALiNls-{self.username}] 调用结果回调函数 (final=False)")
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
                status_code = header.get('status', 'Unknown')
                print(f"[ALiNls-{self.username}] ✗ 任务失败")
                print(f"[ALiNls-{self.username}] 错误代码: {status_code}")
                print(f"[ALiNls-{self.username}] 错误信息: {error_msg}")
                print(f"[ALiNls-{self.username}] 完整错误数据: {data}")

            else:
                print(f"[ALiNls-{self.username}] ⚠ 未知消息类型: {name}")
                print(f"[ALiNls-{self.username}] 完整消息: {data}")

        except json.JSONDecodeError as e:
            print(f"[ALiNls-{self.username}] JSON解析错误: {e}")
            print(f"[ALiNls-{self.username}] 原始消息: {message}")
        except Exception as e:
            print(f"[ALiNls-{self.username}] 处理消息时出错: {e}")
            print(f"[ALiNls-{self.username}] 错误类型: {type(e).__name__}")
            import traceback
            traceback.print_exc()

    # 收到websocket的关闭要求
    def on_close(self, ws, code, msg):
        self.__endding = True
        self.__is_close = True
        print(f"[ALiNls-{self.username}] ✗ WebSocket连接已关闭")
        print(f"[ALiNls-{self.username}] 关闭代码: {code}")
        print(f"[ALiNls-{self.username}] 关闭消息: {msg}")

        # 诊断关闭原因
        if code == 4001:
            print(f"[ALiNls-{self.username}] ❌ Token无效或过期，请检查阿里云配置")
        elif code == 4002:
            print(f"[ALiNls-{self.username}] ❌ 请求参数错误")
        elif code == 4003:
            print(f"[ALiNls-{self.username}] ❌ 认证失败")
        elif code == 1006:
            print(f"[ALiNls-{self.username}] ❌ 连接异常断开，可能是网络问题")
        elif code is None:
            print(f"[ALiNls-{self.username}] ❌ 连接立即断开，可能是Token或网络问题")

    # 收到websocket错误的处理
    def on_error(self, ws, error):
        print(f"[ALiNls-{self.username}] ✗ WebSocket错误: {error}")
        print(f"[ALiNls-{self.username}] 错误类型: {type(error).__name__}")

        # 详细错误信息
        if hasattr(error, 'args'):
            print(f"[ALiNls-{self.username}] 错误参数: {error.args}")

        import traceback
        traceback.print_exc()

        self.started = True  # 避免在aliyun asr出错时，recorder一直等待start状态返回

    # 收到websocket连接建立的处理
    def on_open(self, ws):
        self.__endding = False
        self.__is_close = False  # 重置关闭标志
        print(f"[ALiNls-{self.username}] ✓ WebSocket连接已成功建立")

        # 连接建立后立即发送启动命令
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

        with self.lock:
            self.__frames.append(data)
        print(f"[ALiNls-{self.username}] 启动转录命令已加入发送队列")

        def run(*args):
            sent_packets = 0
            sent_bytes = 0
            start_command_sent = False
            self.__last_send_time = time.time()

            print(f"[ALiNls-{self.username}] 发送线程已启动")

            while self.__endding == False:
                try:
                    current_time = time.time()

                    # 检查是否需要发送保活静音包
                    if (current_time - self.__last_send_time > self.__keepalive_interval and
                            start_command_sent and len(self.__frames) == 0):
                        print(
                            f"[ALiNls-{self.username}] 发送保活静音包 (距上次发送: {current_time - self.__last_send_time:.1f}秒)")
                        ws.send(self.__silence_data,
                                websocket.ABNF.OPCODE_BINARY)
                        self.__last_send_time = current_time

                    if len(self.__frames) > 0:
                        with self.lock:
                            frame = self.__frames.pop(0)

                        if isinstance(frame, dict):
                            message_json = json.dumps(frame)
                            ws.send(message_json)
                            frame_name = frame.get(
                                'header', {}).get('name', 'Unknown')
                            print(
                                f"[ALiNls-{self.username}] → 发送控制消息: {frame_name}")
                            if frame_name == 'StartTranscription':
                                start_command_sent = True
                                print(
                                    f"[ALiNls-{self.username}] ✓ StartTranscription命令已发送")
                            self.__last_send_time = current_time

                        elif isinstance(frame, bytes):
                            # 将大的音频包分割成更小的块
                            chunk_size = 3200  # 100ms的音频数据 (16000*2/10)
                            for i in range(0, len(frame), chunk_size):
                                chunk = frame[i:i+chunk_size]
                                ws.send(chunk, websocket.ABNF.OPCODE_BINARY)
                                self.data += chunk
                                sent_packets += 1
                                sent_bytes += len(chunk)
                                self.__last_send_time = current_time

                                # 控制发送速度，模拟实时音频流
                                time.sleep(0.1)  # 100ms间隔

                            # 分析音频数据
                            import struct
                            if len(frame) >= 2:
                                samples = struct.unpack(
                                    '<' + 'h' * min(4, len(frame)//2), frame[:8])
                                max_sample = max(abs(s)
                                                 for s in samples) if samples else 0

                                print(
                                    f"[ALiNls-{self.username}] → 发送音频块: {len(frame)}字节 -> {len(frame)//chunk_size + 1}个小块, 最大采样值: {max_sample}")

                            # 定期输出统计
                            if sent_packets % 20 == 0:
                                print(
                                    f"[ALiNls-{self.username}] → 已发送音频包: {sent_packets}, 字节: {sent_bytes}, 队列剩余: {len(self.__frames)}")
                    else:
                        time.sleep(0.01)

                except Exception as e:
                    print(f"[ALiNls-{self.username}] 发送数据时出错: {e}")
                    import traceback
                    traceback.print_exc()
                    break

            print(
                f"[ALiNls-{self.username}] 发送线程结束 - 总发送: {sent_packets}包, {sent_bytes}字节")

            # 发送停止命令
            if self.__is_close == False:
                try:
                    frame = {"header": self.__create_header(
                        'StopTranscription')}
                    stop_message = json.dumps(frame)
                    ws.send(stop_message)
                    print(f"[ALiNls-{self.username}] → 发送停止转录命令")
                except Exception as e:
                    print(f"[ALiNls-{self.username}] 发送停止命令时出错: {e}")

        thread.start_new_thread(run, ())

    def __connect(self):
        try:
            print(f"[ALiNls-{self.username}] 开始连接阿里云NLS...")
            print(
                f"[ALiNls-{self.username}] 当前Token: {_token[:20] if _token else 'None'}...")
            print(f"[ALiNls-{self.username}] 连接URL: {self.__URL}")

            if not _token:
                print(f"[ALiNls-{self.username}] 错误: Token为空，无法连接")
                self.__is_close = True
                return

            # 重置连接状态
            self.finalResults = ""
            self.done = False
            self.__endding = False
            self.__is_close = False

            # 清空帧队列
            with self.lock:
                self.__frames.clear()

            # 如果已有WebSocket连接，先关闭
            if self.__ws:
                try:
                    self.__ws.close()
                except:
                    pass
                self.__ws = None

            full_url = self.__URL + '?token=' + _token
            print(f"[ALiNls-{self.username}] 完整连接URL: {full_url}")

            # 创建新的WebSocket连接
            self.__ws = websocket.WebSocketApp(
                full_url,
                on_message=self.on_message,
                on_open=self.on_open,
                on_error=self.on_error,
                on_close=self.on_close
            )

            print(f"[ALiNls-{self.username}] WebSocketApp已创建，开始连接...")

            # 设置连接超时
            self.__ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=30,
                ping_timeout=10
            )

            print(f"[ALiNls-{self.username}] WebSocket连接已结束")

        except Exception as e:
            print(f"[ALiNls-{self.username}] 连接过程中出错: {e}")
            print(f"[ALiNls-{self.username}] 错误类型: {type(e).__name__}")
            self.__is_close = True
            import traceback
            traceback.print_exc()

    def send(self, buf):
        """发送音频数据到阿里云"""
        with self.lock:
            self.__frames.append(buf)
            # 添加音频数据接收日志
            if isinstance(buf, bytes):
                # 分析音频数据特征
                import struct
                if len(buf) >= 2:
                    # 检查前几个采样点
                    samples = struct.unpack(
                        '<' + 'h' * min(8, len(buf)//2), buf[:16])
                    max_sample = max(abs(s) for s in samples) if samples else 0
                    print(
                        f"[ALiNls-{self.username}] ← 接收音频数据: {len(buf)} bytes, 队列长度: {len(self.__frames)}, 最大采样值: {max_sample}")
                else:
                    print(
                        f"[ALiNls-{self.username}] ← 接收音频数据: {len(buf)} bytes, 队列长度: {len(self.__frames)}")
            elif isinstance(buf, dict):
                frame_name = buf.get('header', {}).get('name', 'Unknown')
                print(f"[ALiNls-{self.username}] ← 接收控制消息: {frame_name}")

    def start(self):
        print(f"[ALiNls-{self.username}] 启动ASR连接...")
        print(
            f"[ALiNls-{self.username}] 当前全局Token状态: {_token[:20] if _token else 'None'}...")
        print(
            f"[ALiNls-{self.username}] AppKey: {cfg.key_ali_nls_app_key[:10] if cfg.key_ali_nls_app_key else 'None'}...")

        # 重置实例状态 - 关键修复
        self.__task_id = ''  # 清空TaskID
        self.started = False
        self.done = False
        self.finalResults = ""
        self.__endding = False
        self.__is_close = False
        self.data = b''

        # 清空音频帧队列
        with self.lock:
            self.__frames.clear()

        print(f"[ALiNls-{self.username}] ✓ 实例状态已重置")

        # 启动连接线程
        Thread(target=self.__connect, args=[]).start()

        # 等待连接建立后再发送启动命令
        print(f"[ALiNls-{self.username}] 等待WebSocket连接建立...")

    def end(self):
        print(f"[ALiNls-{self.username}] 结束ASR会话...")
        self.__endding = True

        # 保存音频数据
        if len(self.data) > 0:
            try:
                import os
                os.makedirs('cache_data', exist_ok=True)
                with wave.open('cache_data/input2.wav', 'wb') as wf:
                    n_channels = 1
                    sampwidth = 2
                    wf.setnchannels(n_channels)
                    wf.setsampwidth(sampwidth)
                    wf.setframerate(16000)
                    wf.writeframes(self.data)
                print(
                    f"[ALiNls-{self.username}] ✓ 音频数据已保存: {len(self.data)} bytes -> cache_data/input2.wav")
            except Exception as e:
                print(f"[ALiNls-{self.username}] 保存音频数据时出错: {e}")
        else:
            print(f"[ALiNls-{self.username}] 没有音频数据需要保存")

        self.data = b''
        print(f"[ALiNls-{self.username}] ASR会话已结束")
