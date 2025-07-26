import asyncio
import websockets
import json
import logging
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
        print("client connected", client_id)
        asr_instance = None

        try:
            logger.info(f"Client {client_id} connected")

            async for message in websocket:
                if isinstance(message, str):
                    # 处理控制消息
                    data = json.loads(message)

                    if data.get('action') == 'start':
                        # 启动ASR
                        asr_mode = data.get('mode', cfg.ASR_mode)
                        asr_instance = self._create_asr_instance(
                            asr_mode, client_id)
                        asr_instance.start()
                        self.clients[client_id] = {
                            'websocket': websocket,
                            'asr': asr_instance
                        }
                        await websocket.send(json.dumps({
                            'status': 'started',
                            'mode': asr_mode
                        }))

                    elif data.get('action') == 'stop':
                        # 停止ASR
                        if asr_instance:
                            asr_instance.end()
                            final_result = getattr(
                                asr_instance, 'finalResults', '')
                            await websocket.send(json.dumps({
                                'status': 'stopped',
                                'final_result': final_result
                            }))
                        break

                elif isinstance(message, bytes):
                    # 处理音频数据
                    if asr_instance:
                        asr_instance.send(message)

                        # 检查是否有新的识别结果
                        if hasattr(asr_instance, 'finalResults') and asr_instance.finalResults:
                            await websocket.send(json.dumps({
                                'type': 'result',
                                'text': asr_instance.finalResults,
                                'is_final': asr_instance.done
                            }))

                            if asr_instance.done:
                                asr_instance.done = False

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            logger.error(f"Error handling client {client_id}: {e}")
            await websocket.send(json.dumps({
                'status': 'error',
                'message': str(e)
            }))
        finally:
            # 清理资源
            if client_id in self.clients:
                if asr_instance:
                    asr_instance.end()
                del self.clients[client_id]

    def _create_asr_instance(self, mode, username):
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

            start_server = websockets.serve(
                self.handle_client,
                self.host,
                self.port,
                ping_interval=30,
                ping_timeout=10
            )

            logger.info(
                f"ASR WebSocket server starting on {self.host}:{self.port}")
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
