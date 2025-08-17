#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一的ASR+LLM+TTS WebSocket服务
完全复用现有模块的成功实现，实现零侵入式集成
Phase 1: 集成ASR服务 - 直接复用asr_ws_server.py的成功逻辑
"""

import asyncio
import websockets
import json
import logging
import time
import threading
import queue
import os
from typing import Dict, Any
from threading import Thread, Lock
import subprocess

# 导入现有ASR模块 - 直接复用成功的实现
from asr.ali_nls import ALiNls
from asr.funasr import FunASR
from utils import config_util as cfg, util

# 动态加载TTS模块


def get_tts_speech_class():
    """根据配置动态加载TTS Speech类"""
    if cfg.tts_module == 'ali':
        from tts.ali_tss import Speech
    elif cfg.tts_module == 'gptsovits':
        from tts.gptsovits import Speech
    elif cfg.tts_module == 'gptsovits_v3':
        from tts.gptsovits_v3 import Speech
    elif cfg.tts_module == 'volcano':
        from tts.volcano_tts import Speech
    else:
        from tts.ms_tts_sdk import Speech
    return Speech


logger = logging.getLogger(__name__)


class UnifiedASRLLMTTSService:
    """
    统一的ASR+LLM+TTS服务
    Phase 1: 完全复用现有ASR服务的成功实现
    """

    def __init__(self, host="0.0.0.0", port=10004):
        self.host = host
        self.port = port
        self.clients = {}
        self.server = None
        self.running = False
        self.loop = None  # 保存事件循环引用，供ASR回调使用

    async def start_server(self):
        """启动WebSocket服务器"""
        self.running = True
        self.loop = asyncio.get_event_loop()  # 保存事件循环引用

        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=30,
            ping_timeout=10,
            max_size=1048576  # 1MB
        )
        util.log(1, f"统一ASR+LLM+TTS服务已启动在 {self.host}:{self.port}")

    async def stop_server(self):
        """停止WebSocket服务器"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def handle_client(self, websocket, path):
        """
        处理客户端连接 - 直接复用asr_ws_server.py的成功实现
        Phase 1: 专注于ASR功能，后续阶段集成LLM和TTS
        """
        client_id = f"unified_client_{id(websocket)}"
        client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"

        util.log(1, f"[{client_id}] 新客户端连接 - IP: {client_ip}, Path: {path}")

        # ASR相关变量 - 完全复用现有逻辑
        asr_instance = None
        audio_packet_count = 0
        total_audio_bytes = 0
        first_audio_time = None
        last_audio_time = None

        try:
            async for message in websocket:
                if isinstance(message, str):
                    # 处理控制消息 - 兼容原有协议和新协议
                    data = json.loads(message)
                    logger.info(f"[{client_id}] 收到控制消息: {data}")

                    # 兼容原有ASR协议: {"action": "start", "mode": "ali"}
                    if data.get('action') == 'start':
                        asr_instance = await self._handle_asr_start(client_id, websocket, data, asr_instance)

                    # 兼容新统一协议: {"action": "start_conversation", ...}
                    elif data.get('action') == 'start_conversation':
                        # 转换为ASR协议格式
                        asr_data = {
                            'action': 'start',
                            'mode': data.get('asr_mode', cfg.ASR_mode or 'ali')
                        }
                        asr_instance = await self._handle_asr_start(client_id, websocket, asr_data, asr_instance)

                    # 处理直接文本输入 (用于测试LLM+TTS流程)
                    elif data.get('action') == 'text_input':
                        text = data.get('text', '')
                        if text.strip():
                            logger.info(f"[{client_id}] 收到直接文本输入: {text}")

                            # 发送模拟ASR结果给客户端
                            mock_asr_result = json.dumps({
                                'type': 'asr_result',
                                'text': text,
                                'is_final': True,
                                'timestamp': time.time()
                            })
                            await websocket.send(mock_asr_result)

                            # 直接调用LLM处理
                            def llm_thread():
                                self._process_with_llm(
                                    text, websocket, client_id)
                            threading.Thread(
                                target=llm_thread, daemon=True).start()

                    elif data.get('action') in ['stop', 'stop_conversation']:
                        await self._handle_asr_stop(client_id, websocket, asr_instance, audio_packet_count,
                                                    total_audio_bytes, first_audio_time, last_audio_time)
                        break

                elif isinstance(message, bytes):
                    # 处理音频数据 - 完全复用现有逻辑
                    current_time = time.time()
                    audio_packet_count += 1
                    audio_size = len(message)
                    total_audio_bytes += audio_size

                    # 记录时间统计
                    if first_audio_time is None:
                        first_audio_time = current_time
                    last_audio_time = current_time

                    # ASR音频处理 - 直接复用现有逻辑
                    await self._handle_asr_audio(client_id, websocket, message, asr_instance,
                                                 audio_packet_count, audio_size, current_time,
                                                 first_audio_time, total_audio_bytes)

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"[{client_id}] 客户端断开连接")
        except Exception as e:
            logger.error(f"[{client_id}] 处理客户端时出错: {e}")
            try:
                await websocket.send(json.dumps({
                    'status': 'error',
                    'message': str(e)
                }))
            except Exception:
                pass  # 忽略发送错误消息时的异常
        finally:
            # 清理资源 - 复用现有逻辑
            if audio_packet_count > 0:
                duration = last_audio_time - \
                    first_audio_time if first_audio_time and last_audio_time else 0
                avg_packet_size = total_audio_bytes / audio_packet_count
                logger.info(f"[{client_id}] 会话结束统计 - 总包数: {audio_packet_count}, "
                            f"总字节: {total_audio_bytes}, 平均包大小: {avg_packet_size:.1f}B, "
                            f"总持续时间: {duration:.2f}s")

            if client_id in self.clients:
                if asr_instance:
                    asr_instance.end()
                del self.clients[client_id]
                logger.info(f"[{client_id}] 客户端资源已清理")

    async def _handle_asr_start(self, client_id, websocket, data, current_asr_instance):
        """处理ASR启动 - 完全复用asr_ws_server.py逻辑"""
        # 如果已有实例，先清理
        if current_asr_instance:
            logger.info(f"[{client_id}] 清理现有ASR实例")
            current_asr_instance.end()
            current_asr_instance = None

        # 创建新的ASR实例
        asr_mode = data.get('mode', cfg.ASR_mode)

        try:
            logger.info(f"[{client_id}] 🚀 创建新ASR实例 - 模式: {asr_mode}")
            asr_instance = self._create_asr_instance(asr_mode, client_id)

            # 设置回调函数 - 直接复用现有逻辑
            def result_callback(text, is_final):
                try:
                    logger.info(
                        f"[{client_id}] 收到ASR结果: text='{text}', is_final={is_final}")

                    # 创建结果消息 - 与现有ASR服务格式完全一致
                    result_message = json.dumps({
                        'type': 'result',
                        'text': text,
                        'is_final': is_final,
                        'timestamp': time.time()
                    })

                    # 线程安全发送消息 - 复用现有机制
                    future = asyncio.run_coroutine_threadsafe(
                        websocket.send(result_message),
                        self.loop
                    )

                    try:
                        future.result(timeout=1.0)
                        logger.info(f"[{client_id}] ASR结果已发送到客户端")

                        # Phase 2: 集成LLM处理
                        if is_final and text.strip():
                            logger.info(f"[{client_id}] 最终结果，启动LLM处理: {text}")
                            # 在独立线程中调用LLM，避免阻塞ASR

                            def llm_thread():
                                self._process_with_llm(
                                    text, websocket, client_id)
                            threading.Thread(
                                target=llm_thread, daemon=True).start()

                    except Exception as send_error:
                        logger.error(f"[{client_id}] 发送ASR结果失败: {send_error}")

                except Exception as callback_error:
                    logger.error(f"[{client_id}] 回调函数执行出错: {callback_error}")
                    import traceback
                    traceback.print_exc()

            asr_instance.set_result_callback(result_callback)

            # 启动ASR实例
            logger.info(f"[{client_id}] 🎯 启动ASR实例...")
            asr_instance.start()

            # 等待连接建立 - 复用现有等待逻辑
            max_wait_time = 15.0
            wait_start = time.time()
            connection_established = False

            while (time.time() - wait_start) < max_wait_time:
                if asr_instance.started:
                    # 对于阿里云ASR，额外检查连接状态
                    if hasattr(asr_instance, '_ALiNls__is_close'):
                        if not asr_instance._ALiNls__is_close:
                            connection_established = True
                            logger.info(f"[{client_id}] ✅ ASR连接建立成功")
                            break
                        else:
                            logger.warning(f"[{client_id}] 检测到ASR连接已断开")
                            break
                    else:
                        connection_established = True
                        logger.info(f"[{client_id}] ✅ ASR连接建立成功")
                        break
                await asyncio.sleep(0.1)

            if not connection_established:
                logger.error(f"[{client_id}] ASR连接建立失败，超时或连接断开")
                await websocket.send(json.dumps({
                    'status': 'error',
                    'message': 'ASR connection failed - timeout or disconnected'
                }))
                return None

            # 更新客户端状态
            self.clients[client_id] = {
                'websocket': websocket,
                'asr': asr_instance,
                'started': True,
                'asr_started': True,
                'mode': asr_mode
            }

            logger.info(f"[{client_id}] ASR实例创建成功，模式: {asr_mode}")
            await websocket.send(json.dumps({
                'status': 'ready',
                'mode': asr_mode,
                'message': 'ASR instance created and connected successfully'
            }))

            return asr_instance

        except Exception as e:
            logger.error(f"[{client_id}] 创建ASR实例失败: {e}")
            await websocket.send(json.dumps({
                'status': 'error',
                'message': f'Failed to create ASR instance: {str(e)}'
            }))
            return None

    async def _handle_asr_stop(self, client_id, websocket, asr_instance, audio_packet_count,
                               total_audio_bytes, first_audio_time, last_audio_time):
        """处理ASR停止"""
        logger.info(f"[{client_id}] 收到停止命令")
        if asr_instance:
            asr_instance.end()

        # 输出音频统计信息
        if audio_packet_count > 0:
            duration = last_audio_time - \
                first_audio_time if first_audio_time and last_audio_time else 0
            logger.info(f"[{client_id}] 音频统计 - 包数: {audio_packet_count}, "
                        f"总字节: {total_audio_bytes}, 持续时间: {duration:.2f}秒")

    async def _handle_asr_audio(self, client_id, websocket, message, asr_instance,
                                audio_packet_count, audio_size, current_time,
                                first_audio_time, total_audio_bytes):
        """处理ASR音频数据 - 复用现有逻辑"""
        # 检查ASR实例状态
        if not asr_instance:
            logger.warning(
                f"[{client_id}] 收到音频数据但ASR实例不存在 - 包#{audio_packet_count}")
            return

        # 检查ASR连接状态 - 复用现有重连逻辑
        if hasattr(asr_instance, '_ALiNls__is_close') and asr_instance._ALiNls__is_close:
            logger.warning(f"[{client_id}] ASR连接已断开，尝试重新连接...")
            # TODO: 实现重连逻辑（如果需要）
            return

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

    def _create_asr_instance(self, mode, username):
        """创建ASR实例 - 直接复用现有逻辑"""
        logger.info(f"创建ASR实例 - 模式: {mode}, 用户: {username}")
        if mode == "ali":
            return ALiNls(username)
        elif mode in ["funasr", "sensevoice"]:
            return FunASR(username)
        else:
            raise ValueError(f"Unsupported ASR mode: {mode}")

    def _process_with_llm(self, text: str, websocket, client_id: str):
        """使用LLM处理文本 - 监听stream_manager输出"""
        logger.info(f"[{client_id}] 开始LLM处理: {text}")

        try:
            # 获取用户名
            username = self.clients.get(client_id, {}).get(
                'username', 'UnifiedUser')

            # 延迟导入避免循环依赖
            from core import stream_manager

            # 保存原始回调
            original_write_sentence = stream_manager.new_instance().write_sentence

            # 创建自定义回调来监听LLM输出
            def llm_output_callback(cb_username, sentence):
                # 只处理当前用户的输出
                if cb_username == username:
                    try:
                        # 解析流式输出标记
                        is_first = "_<isfirst>" in sentence
                        is_end = "_<isend>" in sentence

                        # 清理标记
                        clean_sentence = sentence.replace(
                            "_<isfirst>", "").replace("_<isend>", "")

                        if clean_sentence.strip():
                            logger.info(
                                f"[{client_id}] LLM输出: {clean_sentence}")

                            # 发送LLM结果给客户端
                            loop = self.loop
                            if loop and not loop.is_closed():
                                asyncio.run_coroutine_threadsafe(
                                    self._send_llm_result(
                                        websocket, clean_sentence, is_first, is_end),
                                    loop
                                )

                            # Phase 3: 调用TTS合成
                            self._process_with_tts(
                                clean_sentence, is_first, is_end, websocket, client_id)

                    except Exception as e:
                        logger.error(f"[{client_id}] LLM回调处理出错: {e}")
                else:
                    # 非当前用户的消息，调用原始回调
                    original_write_sentence(cb_username, sentence)

            # 临时替换回调
            stream_manager.new_instance().write_sentence = llm_output_callback

            try:
                # 延迟导入避免循环依赖
                from llm.nlp_cognitive_stream import question

                # 调用LLM处理
                logger.info(
                    f"[{client_id}] 调用LLM question函数: username={username}, text={text}")
                question(text, username)

            finally:
                # 恢复原始回调
                stream_manager.new_instance().write_sentence = original_write_sentence
                logger.info(f"[{client_id}] LLM处理完成，已恢复原始回调")

        except Exception as e:
            logger.error(f"[{client_id}] LLM处理出错: {e}")
            import traceback
            logger.error(f"[{client_id}] 错误详情: {traceback.format_exc()}")

            # 发送错误消息给客户端
            if websocket and not websocket.closed:
                try:
                    loop = self.loop
                    if loop and not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(
                            self._send_error(websocket, f"LLM处理失败: {str(e)}"),
                            loop
                        )
                except Exception as send_error:
                    logger.error(f"[{client_id}] 发送LLM错误消息失败: {send_error}")

    async def _send_llm_result(self, websocket, text: str, is_first: bool, is_end: bool):
        """发送LLM结果给客户端"""
        try:
            result_message = json.dumps({
                'type': 'llm_result',
                'text': text,
                'is_first': is_first,
                'is_end': is_end,
                'timestamp': time.time()
            })
            await websocket.send(result_message)
            logger.info(f"LLM结果已发送: {text}")

        except Exception as e:
            logger.error(f"发送LLM结果失败: {e}")

    async def _send_error(self, websocket, error_message: str):
        """发送错误消息给客户端"""
        try:
            error_msg = json.dumps({
                'type': 'error',
                'message': error_message,
                'timestamp': time.time()
            })
            await websocket.send(error_msg)
        except Exception as e:
            logger.error(f"发送错误消息失败: {e}")

    def _process_with_tts(self, text: str, is_first: bool, is_end: bool, websocket, client_id: str):
        """使用TTS合成语音 - Phase 3: 完全复用现有TTS模块"""
        logger.info(f"[{client_id}] 开始TTS处理: {text}")

        try:
            if not text.strip():
                logger.warning(f"[{client_id}] TTS文本为空，跳过")
                return

            # 动态获取TTS Speech类
            speech_class = self._get_tts_speech_class()
            if not speech_class:
                logger.error(f"[{client_id}] TTS模块未配置或加载失败")
                return

            # 创建TTS实例
            logger.info(f"[{client_id}] 创建TTS实例: {speech_class.__name__}")
            speech = speech_class()

            # 连接TTS服务（如果需要）
            if hasattr(speech, 'connect'):
                speech.connect()

            # 生成语音文件
            style = "normal"  # 默认风格
            logger.info(
                f"[{client_id}] 开始TTS合成: text='{text}', style='{style}'")

            # 🔧 新增：TTS重试机制
            max_retries = 3
            audio_file = None

            for attempt in range(max_retries):
                try:
                    audio_file = speech.to_sample(text, style)

                    if audio_file and os.path.exists(audio_file):
                        # 🔧 新增：音频质量验证
                        quality_check = self._validate_tts_audio(
                            audio_file, text)
                        if quality_check['is_valid']:
                            logger.info(
                                f"[{client_id}] TTS合成成功，质量检查通过: {audio_file}")
                            break
                        else:
                            logger.warning(
                                f"[{client_id}] TTS音频质量检查失败: {quality_check['reason']}")
                            if attempt < max_retries - 1:
                                logger.info(
                                    f"[{client_id}] 尝试重新生成 (尝试 {attempt + 1}/{max_retries})")
                                # 删除质量不合格的文件
                                try:
                                    os.remove(audio_file)
                                except:
                                    pass
                                audio_file = None
                                continue
                    else:
                        logger.warning(f"[{client_id}] TTS返回空文件或文件不存在")

                except Exception as e:
                    logger.error(
                        f"[{client_id}] TTS合成异常 (尝试 {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"[{client_id}] 尝试重新生成...")
                        continue
                    else:
                        raise e

            if audio_file and os.path.exists(audio_file):
                logger.info(
                    f"[{client_id}] TTS合成成功，生成文件: {audio_file}")

                # 异步发送音频文件
                loop = self.loop
                if loop and not loop.is_closed():
                    asyncio.run_coroutine_threadsafe(
                        self._send_audio_file(
                            websocket, audio_file, text, is_first, is_end, client_id),
                        loop
                    )
                else:
                    logger.error(f"[{client_id}] 事件循环不可用，无法发送音频文件")
            else:
                logger.error(f"[{client_id}] TTS合成失败，未生成音频文件")
                # 🔧 新增：发送TTS失败通知
                self._send_tts_failure_notification(websocket, text, client_id)

        except Exception as e:
            logger.error(f"[{client_id}] TTS处理出错: {e}")
            import traceback
            logger.error(f"[{client_id}] TTS错误详情: {traceback.format_exc()}")

            # 🔧 新增：发送TTS错误通知
            self._send_tts_failure_notification(websocket, text, client_id)

    # 🔧 新增：TTS音频质量验证
    def _validate_tts_audio(self, audio_file: str, text: str) -> dict:
        """验证TTS生成的音频质量"""
        try:
            if not os.path.exists(audio_file):
                return {'is_valid': False, 'reason': '文件不存在'}

            file_size = os.path.getsize(audio_file)

            # 基本文件大小检查
            if file_size < 1024:  # 小于1KB
                return {'is_valid': False, 'reason': f'文件过小: {file_size} bytes'}

            # 根据文本长度估算最小文件大小
            min_expected_size = len(text) * 100  # 粗略估算：每个字符100字节
            if file_size < min_expected_size:
                return {'is_valid': False, 'reason': f'文件大小异常: {file_size} bytes, 期望至少 {min_expected_size} bytes'}

            # 检查文件头
            with open(audio_file, 'rb') as f:
                header = f.read(16)

            file_ext = os.path.splitext(audio_file)[1].lower()

            if file_ext == '.wav':
                # 检查WAV文件头
                if header[:4] != b'RIFF' or header[8:12] != b'WAVE':
                    return {'is_valid': False, 'reason': 'WAV文件头无效'}
            elif file_ext == '.mp3':
                # 检查MP3文件头
                if not (header[0] == 0xFF and (header[1] & 0xE0) == 0xE0):
                    return {'is_valid': False, 'reason': 'MP3文件头无效'}

            # 获取音频详细信息
            audio_info = self._get_audio_info(audio_file)

            # 检查采样率
            if audio_info.get('sample_rate', 0) < 8000:
                return {'is_valid': False, 'reason': f'采样率过低: {audio_info.get("sample_rate")}Hz'}

            # 检查时长
            duration = audio_info.get('duration', 0)
            if duration < 0.1:  # 少于100ms
                return {'is_valid': False, 'reason': f'音频时长过短: {duration:.3f}秒'}

            return {
                'is_valid': True,
                'file_size': file_size,
                'format': file_ext,
                'sample_rate': audio_info.get('sample_rate'),
                'channels': audio_info.get('channels'),
                'duration': duration,
                'reason': '质量检查通过'
            }

        except Exception as e:
            return {'is_valid': False, 'reason': f'验证异常: {str(e)}'}

    # 🔧 新增：发送TTS失败通知
    def _send_tts_failure_notification(self, websocket, text: str, client_id: str):
        """发送TTS失败通知给客户端"""
        try:
            failure_message = json.dumps({
                'type': 'tts_failure',
                'text': text,
                'reason': 'TTS合成失败',
                'timestamp': time.time()
            })

            loop = self.loop
            if loop and not loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    websocket.send(failure_message),
                    loop
                )
                logger.info(f"[{client_id}] TTS失败通知已发送")
            else:
                logger.error(f"[{client_id}] 事件循环不可用，无法发送TTS失败通知")

        except Exception as e:
            logger.error(f"[{client_id}] 发送TTS失败通知异常: {e}")

    def _get_tts_speech_class(self):
        """动态获取TTS Speech类 - 完全复用现有配置逻辑"""
        try:
            # 根据配置选择TTS模块
            tts_module = cfg.tts_module

            if tts_module == "ali":
                from tts.ali_tss import Speech
                return Speech
            elif tts_module == "ms":
                from tts.ms_tts_sdk import Speech
                return Speech
            elif tts_module == "gptsovits":
                from tts.gptsovits import Speech
                return Speech
            elif tts_module == "volcano":
                from tts.volcano_tts import Speech
                return Speech
            else:
                logger.warning(f"未知TTS模块: {tts_module}，使用默认ms模块")
                from tts.ms_tts_sdk import Speech
                return Speech

        except Exception as e:
            logger.error(f"加载TTS Speech类失败: {e}")
            return None

    async def _send_audio_file(self, websocket, audio_file: str, text: str, is_first: bool, is_end: bool, client_id: str):
        """发送音频文件给客户端"""
        try:
            if not os.path.exists(audio_file):
                logger.error(f"[{client_id}] 音频文件不存在: {audio_file}")
                return

            # 🔧 检查音频文件格式
            file_ext = os.path.splitext(audio_file)[1].lower()
            if file_ext == '.mp3':
                logger.warning(
                    f"[{client_id}] 检测到MP3格式，客户端可能无法处理: {audio_file}")
                # 可以在这里添加格式转换逻辑

            # 读取音频文件
            with open(audio_file, 'rb') as f:
                audio_data = f.read()

            if not audio_data:
                logger.error(f"[{client_id}] 音频文件为空: {audio_file}")
                return

            # 🔧 优化：根据音频格式调整传输参数
            if file_ext == '.wav':
                # WAV格式：使用较大分块，减少发送次数
                chunk_size = 16384  # 16KB chunks
                send_delay = 0.005   # 5ms延迟
            elif file_ext == '.mp3':
                # MP3格式：使用较小分块，避免解码问题
                chunk_size = 4096    # 4KB chunks
                send_delay = 0.010   # 10ms延迟
            else:
                # 其他格式：默认参数
                chunk_size = 8192    # 8KB chunks
                send_delay = 0.008   # 8ms延迟

            # 🔧 优化：添加音频质量信息
            audio_info = self._get_audio_info(audio_file)

            # 🔧 添加音频格式信息到元数据
            start_message = json.dumps({
                'type': 'audio_start',
                'text': text,
                'is_first': is_first,
                'total_size': len(audio_data),
                'format': file_ext[1:] if file_ext else 'unknown',  # 添加格式信息
                'sample_rate': audio_info.get('sample_rate', 44100),  # 使用实际采样率
                'channels': audio_info.get('channels', 1),         # 使用实际声道数
                # 添加位深度
                'bits_per_sample': audio_info.get('bits_per_sample', 16),
                'duration': audio_info.get('duration', 0),         # 添加时长信息
                'chunk_size': chunk_size,                         # 添加分块大小信息
                'timestamp': time.time()
            })
            await websocket.send(start_message)

            # 🔧 优化：智能分块发送
            for i in range(0, len(audio_data), chunk_size):
                chunk = audio_data[i:i + chunk_size]
                await websocket.send(chunk)

                # 动态调整发送延迟
                if i % (chunk_size * 10) == 0:  # 每10个块检查一次
                    await asyncio.sleep(send_delay * 2)  # 稍微停顿
                else:
                    await asyncio.sleep(send_delay)

            # 发送音频结束元数据
            end_message = json.dumps({
                'type': 'audio_end',
                'text': text,
                'is_end': is_end,
                'total_size': len(audio_data),
                'format': file_ext[1:] if file_ext else 'unknown',
                'sample_rate': audio_info.get('sample_rate', 44100),
                'channels': audio_info.get('channels', 1),
                'bits_per_sample': audio_info.get('bits_per_sample', 16),
                'duration': audio_info.get('duration', 0),
                'timestamp': time.time()
            })
            await websocket.send(end_message)

            logger.info(
                f"[{client_id}] 音频文件发送完成: {audio_file} ({len(audio_data)} bytes, {file_ext})")

            # 清理临时文件
            try:
                os.remove(audio_file)
                logger.info(f"[{client_id}] 临时音频文件已删除: {audio_file}")
            except Exception as e:
                logger.warning(f"[{client_id}] 删除临时文件失败: {e}")

        except Exception as e:
            logger.error(f"[{client_id}] 发送音频文件失败: {e}")

    # 🔧 新增：获取音频文件信息
    def _get_audio_info(self, audio_file: str) -> dict:
        """获取音频文件信息"""
        try:
            if audio_file.endswith('.wav'):
                import wave
                with wave.open(audio_file, 'rb') as wav:
                    return {
                        'sample_rate': wav.getframerate(),
                        'channels': wav.getnchannels(),
                        'bits_per_sample': wav.getsampwidth() * 8,
                        'duration': wav.getnframes() / wav.getframerate()
                    }
            elif audio_file.endswith('.mp3'):
                # 使用ffprobe获取MP3信息
                try:
                    result = subprocess.run([
                        'ffprobe', '-v', 'quiet', '-print_format', 'json',
                        '-show_format', '-show_streams', audio_file
                    ], capture_output=True, text=True, check=True)

                    info = json.loads(result.stdout)
                    if 'streams' in info and len(info['streams']) > 0:
                        stream = info['streams'][0]
                        return {
                            'sample_rate': int(stream.get('sample_rate', 44100)),
                            'channels': int(stream.get('channels', 1)),
                            'bits_per_sample': 16,  # MP3通常是16位
                            'duration': float(info['format'].get('duration', 0))
                        }
                except Exception:
                    pass

            # 默认值
            return {
                'sample_rate': 44100,
                'channels': 1,
                'bits_per_sample': 16,
                'duration': 0
            }

        except Exception as e:
            logger.warning(f"获取音频信息失败: {e}")
            return {
                'sample_rate': 44100,
                'channels': 1,
                'bits_per_sample': 16,
                'duration': 0
            }


class UnifiedClientSession:
    """统一服务的客户端会话"""

    def __init__(self, client_id: str, websocket, config: Dict[str, Any]):
        self.client_id = client_id
        self.websocket = websocket
        self.config = config
        self.conversation_active = False

        # 组件实例
        self.asr_instance = None
        self.tts_instance = None

        # 状态管理
        self.username = "User"
        self.asr_mode = "ali"  # 默认使用阿里云ASR
        self.current_conversation_id = None

        # 音频统计
        self.audio_packet_count = 0
        self.total_audio_bytes = 0
        self.conversation_start_time = None

        # 流式处理队列
        self.text_queue = queue.Queue()
        self.audio_queue = queue.Queue()
        self.processing_lock = Lock()

    async def start_conversation(self, data: Dict[str, Any]):
        """开始对话"""
        try:
            self.username = data.get('username', 'User')
            self.asr_mode = data.get('asr_mode', cfg.ASR_mode or 'ali')
            self.current_conversation_id = data.get(
                'conversation_id', f"conv_{int(time.time())}")

            # 初始化ASR
            await self._init_asr()

            # 初始化TTS
            await self._init_tts()

            # 启动处理线程
            self._start_processing_threads()

            self.conversation_active = True
            self.conversation_start_time = time.time()

            await self.send_response({
                'status': 'success',
                'message': '对话已开始',
                'conversation_id': self.current_conversation_id,
                'asr_mode': self.asr_mode,
                'tts_mode': cfg.tts_module
            })

            util.log(
                1, f"[{self.client_id}] 对话已开始 - 用户: {self.username}, ASR: {self.asr_mode}")

        except Exception as e:
            await self.send_response({
                'status': 'error',
                'message': f'启动对话失败: {str(e)}'
            })

    async def stop_conversation(self):
        """停止对话"""
        self.conversation_active = False

        # 清理ASR
        if self.asr_instance:
            try:
                self.asr_instance.end()
            except Exception as e:
                util.log(1, f"[{self.client_id}] 清理ASR时出错: {e}")

        # 生成会话统计
        duration = time.time() - self.conversation_start_time if self.conversation_start_time else 0
        stats = {
            'conversation_id': self.current_conversation_id,
            'duration': duration,
            'audio_packets': self.audio_packet_count,
            'total_bytes': self.total_audio_bytes
        }

        await self.send_response({
            'status': 'success',
            'message': '对话已结束',
            'stats': stats
        })

        util.log(1, f"[{self.client_id}] 对话已结束 - 持续时间: {duration:.2f}s")

    async def _init_asr(self):
        """初始化ASR实例"""
        try:
            if self.asr_mode == "ali":
                self.asr_instance = ALiNls(self.username)
            elif self.asr_mode in ["funasr", "sensevoice"]:
                self.asr_instance = FunASR(self.username)
            else:
                raise ValueError(f"不支持的ASR模式: {self.asr_mode}")

            # 设置ASR结果回调
            original_on_message = self.asr_instance.on_message

            def asr_result_callback(ws, message):
                # 调用原始回调
                original_on_message(ws, message)
                # 将识别结果放入队列
                self.text_queue.put(message)

            self.asr_instance.on_message = asr_result_callback
            self.asr_instance.start()

            util.log(1, f"[{self.client_id}] ASR实例已初始化: {self.asr_mode}")

        except Exception as e:
            raise Exception(f"初始化ASR失败: {str(e)}")

    async def _init_tts(self):
        """初始化TTS实例"""
        try:
            Speech = get_tts_speech_class()
            self.tts_instance = Speech()
            self.tts_instance.connect()

            util.log(1, f"[{self.client_id}] TTS实例已初始化: {cfg.tts_module}")

        except Exception as e:
            raise Exception(f"初始化TTS失败: {str(e)}")

    def _start_processing_threads(self):
        """启动处理线程"""
        # 启动文本处理线程
        text_thread = Thread(target=self._text_processing_worker, daemon=True)
        text_thread.start()

        # 启动音频发送线程
        audio_thread = Thread(target=self._audio_sending_worker, daemon=True)
        audio_thread.start()

    def _text_processing_worker(self):
        """文本处理工作线程"""
        while self.conversation_active:
            try:
                # 等待ASR识别结果
                text = self.text_queue.get(timeout=1.0)
                if text and text.strip():
                    util.log(1, f"[{self.client_id}] ASR识别结果: {text}")

                    # 发送ASR结果给客户端
                    asyncio.create_task(self.send_response({
                        'type': 'asr_result',
                        'text': text,
                        'timestamp': time.time()
                    }))

                    # 注意：UnifiedClientSession不再需要处理LLM逻辑
                    # LLM处理已经在UnifiedASRLLMTTSService中完成
                    util.log(1, f"[{self.client_id}] ASR结果: {text} (由主服务处理)")

            except queue.Empty:
                continue
            except Exception as e:
                util.log(1, f"[{self.client_id}] 文本处理出错: {e}")

    # 注意：不再在UnifiedClientSession中定义LLM/TTS方法
    # 这些方法已经在UnifiedASRLLMTTSService类中定义

    async def process_audio(self, audio_data: bytes):
        """处理音频数据"""
        self.audio_packet_count += 1
        self.total_audio_bytes += len(audio_data)

        # 发送给ASR
        if self.asr_instance:
            self.asr_instance.send(audio_data)

        # 定期输出统计
        if self.audio_packet_count % 100 == 0:
            duration = time.time() - self.conversation_start_time
            data_rate = self.total_audio_bytes / duration if duration > 0 else 0
            util.log(1, f"[{self.client_id}] 音频统计 - 包数: {self.audio_packet_count}, "
                     f"总字节: {self.total_audio_bytes}, 速率: {data_rate:.1f}B/s")

    async def send_response(self, data: Dict[str, Any]):
        """发送响应给客户端"""
        try:
            await self.websocket.send(json.dumps(data))
        except Exception as e:
            util.log(1, f"[{self.client_id}] 发送响应失败: {e}")

    async def send_status(self):
        """发送状态信息"""
        status = {
            'status': 'success',
            'conversation_active': self.conversation_active,
            'client_id': self.client_id,
            'username': self.username,
            'asr_mode': self.asr_mode,
            'tts_mode': cfg.tts_module,
            'conversation_id': self.current_conversation_id,
            'stats': {
                'audio_packets': self.audio_packet_count,
                'total_bytes': self.total_audio_bytes,
                'duration': time.time() - self.conversation_start_time if self.conversation_start_time else 0
            }
        }
        await self.send_response(status)

    async def cleanup(self):
        """清理会话资源"""
        self.conversation_active = False

        if self.asr_instance:
            try:
                self.asr_instance.end()
            except Exception:
                pass


# 全局服务实例
_unified_service_instance = None


def get_unified_service():
    """获取统一服务实例"""
    global _unified_service_instance
    if _unified_service_instance is None:
        _unified_service_instance = UnifiedASRLLMTTSService()
    return _unified_service_instance


def start_unified_service():
    """启动统一服务"""
    service = get_unified_service()

    def run_server():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(service.start_server())
            loop.run_forever()
        except Exception as e:
            util.log(1, f"统一服务运行出错: {e}")
        finally:
            loop.close()

    thread = Thread(target=run_server, daemon=True)
    thread.start()

    return service


if __name__ == "__main__":
    # 测试启动
    cfg.load_config()
    start_unified_service()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("服务已停止")
