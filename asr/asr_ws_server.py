import asyncio
import websockets
import json
import logging
import time
from threading import Thread
from asr.ali_nls import ALiNls
from asr.funasr import FunASR
from utils import config_util as cfg

logger = logging.getLogger(__name__)


class ASRWebSocketServer:
    def __init__(self, host="0.0.0.0", port=10199):
        self.host = host
        self.port = port
        self.clients = {}

    async def handle_client(self, websocket, path):
        client_id = f"asr_client_{id(websocket)}"
        asr_instance = None
        audio_packet_count = 0
        total_audio_bytes = 0
        first_audio_time = None
        last_audio_time = None

        try:
            async for message in websocket:
                if isinstance(message, str):
                    data = json.loads(message)
                    if data.get('action') == 'start':
                        # 创建ASR实例
                        asr_mode = data.get('mode', cfg.ASR_mode)
                        asr_instance = self._create_asr_instance(
                            asr_mode, client_id)

                        # 简化的结果回调函数
                        def result_callback(text, is_final):
                            try:
                                logger.info(
                                    f"[{client_id}] 收到ASR结果: text='{text}', is_final={is_final}")

                                # 创建结果消息
                                result_message = json.dumps({
                                    'type': 'result',
                                    'text': text,
                                    'is_final': is_final,
                                    'timestamp': time.time()
                                })

                                # 使用线程安全的方式发送消息
                                future = asyncio.run_coroutine_threadsafe(
                                    websocket.send(result_message),
                                    self.loop
                                )

                                # 等待发送完成，设置超时
                                try:
                                    future.result(timeout=1.0)
                                    logger.info(f"[{client_id}] ASR结果已发送到客户端")
                                except Exception as send_error:
                                    logger.error(
                                        f"[{client_id}] 发送ASR结果失败: {send_error}")

                            except Exception as callback_error:
                                logger.error(
                                    f"[{client_id}] 回调函数执行出错: {callback_error}")
                                import traceback
                                traceback.print_exc()

                        # 设置回调函数
                        logger.info(f"[{client_id}] 设置ASR结果回调函数")
                        asr_instance.set_result_callback(result_callback)

                        self.clients[client_id] = {
                            'websocket': websocket,
                            'asr': asr_instance,
                            'started': False,
                            'asr_started': False
                        }

                        logger.info(f"[{client_id}] ASR实例已创建，模式: {asr_mode}")
                        await websocket.send(json.dumps({
                            'status': 'ready',
                            'mode': asr_mode,
                            'message': 'ASR instance created, waiting for audio data'
                        }))

                    elif data.get('action') == 'stop':
                        logger.info(f"[{client_id}] 收到停止命令")
                        if asr_instance:
                            asr_instance.end()

                        # 输出音频统计信息
                        if audio_packet_count > 0:
                            duration = last_audio_time - \
                                first_audio_time if first_audio_time and last_audio_time else 0
                            logger.info(f"[{client_id}] 音频统计 - 包数: {audio_packet_count}, "
                                        f"总字节: {total_audio_bytes}, 持续时间: {duration:.2f}秒")
                        break

                elif isinstance(message, bytes):
                    current_time = time.time()
                    audio_packet_count += 1
                    audio_size = len(message)
                    total_audio_bytes += audio_size

                    # 记录第一个和最后一个音频包的时间
                    if first_audio_time is None:
                        first_audio_time = current_time
                    last_audio_time = current_time

                    # 只在第一次收到音频数据时启动ASR
                    if (asr_instance and
                        client_id in self.clients and
                            not self.clients[client_id].get('asr_started', False)):

                        logger.info(
                            f"[{client_id}] 收到第一个音频包，启动ASR - 大小: {audio_size} bytes")

                        # 启动ASR
                        asr_instance.start()

                        # 等待ASR启动完成
                        max_wait_time = 5.0  # 最多等待5秒
                        wait_start = time.time()
                        while not asr_instance.started and (time.time() - wait_start) < max_wait_time:
                            await asyncio.sleep(0.01)

                        if asr_instance.started:
                            logger.info(f"[{client_id}] ASR实例已启动成功")
                            # 标记已启动，避免重复启动
                            self.clients[client_id]['asr_started'] = True

                            # 发送启动成功消息
                            await websocket.send(json.dumps({
                                'status': 'started',
                                'message': 'ASR started, processing audio'
                            }))
                        else:
                            logger.error(f"[{client_id}] ASR启动超时")
                            await websocket.send(json.dumps({
                                'status': 'error',
                                'message': 'ASR startup timeout'
                            }))
                            continue

                    # 发送音频数据
                    if (asr_instance and
                        client_id in self.clients and
                            self.clients[client_id].get('asr_started', False)):

                        asr_instance.send(message)

                        # 定期输出音频接收统计
                        if audio_packet_count % 50 == 0:
                            duration = current_time - first_audio_time
                            avg_packet_size = total_audio_bytes / audio_packet_count
                            data_rate = total_audio_bytes / duration if duration > 0 else 0

                            logger.info(f"[{client_id}] 音频接收中 - 包#{audio_packet_count}, "
                                        f"当前包: {audio_size}B, 平均包大小: {avg_packet_size:.1f}B, "
                                        f"数据速率: {data_rate:.1f}B/s, 持续: {duration:.1f}s")
                    else:
                        logger.warning(
                            f"[{client_id}] 收到音频数据但ASR未启动 - 包#{audio_packet_count}, 大小: {audio_size} bytes")

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"[{client_id}] 客户端断开连接")
        except Exception as e:
            logger.error(f"[{client_id}] 处理客户端时出错: {e}")
            try:
                await websocket.send(json.dumps({
                    'status': 'error',
                    'message': str(e)
                }))
            except:
                pass
        finally:
            # 输出最终统计
            if audio_packet_count > 0:
                duration = last_audio_time - \
                    first_audio_time if first_audio_time and last_audio_time else 0
                avg_packet_size = total_audio_bytes / audio_packet_count
                logger.info(f"[{client_id}] 会话结束统计 - 总包数: {audio_packet_count}, "
                            f"总字节: {total_audio_bytes}, 平均包大小: {avg_packet_size:.1f}B, "
                            f"总持续时间: {duration:.2f}s")

            # 清理资源
            if client_id in self.clients:
                if asr_instance:
                    asr_instance.end()
                del self.clients[client_id]
                logger.info(f"[{client_id}] 客户端资源已清理")

    def _create_asr_instance(self, mode, username):
        logger.info(f"创建ASR实例 - 模式: {mode}, 用户: {username}")
        """创建ASR实例"""
        if mode == "ali":
            return ALiNls(username)
        elif mode in ["funasr", "sensevoice"]:
            return FunASR(username)
        else:
            raise ValueError(f"Unsupported ASR mode: {mode}")

    def start_server(self):
        """启动WebSocket服务器"""
        def run_server():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.loop = loop  # 保存loop引用供回调使用

            start_server = websockets.serve(
                self.handle_client,
                self.host,
                self.port,
                ping_interval=30,
                ping_timeout=10
            )

            logger.info(
                f"ASR WebSocket服务器启动 - {self.host}:{self.port}")
            loop.run_until_complete(start_server)
            loop.run_forever()

        server_thread = Thread(target=run_server, daemon=True)
        server_thread.start()
        return server_thread


# 全局实例
_asr_server_instance = None


def get_asr_server(host="0.0.0.0", port=10199):
    global _asr_server_instance
    if _asr_server_instance is None:
        _asr_server_instance = ASRWebSocketServer(host, port)
    return _asr_server_instance
