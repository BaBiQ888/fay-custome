#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一ASR+LLM+TTS服务的客户端测试
演示如何使用WebSocket接口进行完整的对话流水线
"""

import asyncio
import websockets
import json
import pyaudio
import time
import threading
import queue
import wave
import io
import math
import struct
import os
import numpy as np
from typing import Optional


class VirtualAudioGenerator:
    """虚拟音频数据生成器"""

    def __init__(self, sample_rate=16000, channels=1, chunk_size=1024):
        self.sample_rate = sample_rate
        self.channels = channels
        self.chunk_size = chunk_size
        self.time_position = 0.0

    def generate_silence(self, duration_seconds=1.0):
        """生成静音数据"""
        num_samples = int(self.sample_rate * duration_seconds)
        silence_data = np.zeros(num_samples, dtype=np.int16)
        return silence_data.tobytes()

    def generate_sine_wave(self, frequency=440, duration_seconds=2.0, amplitude=0.3):
        """生成正弦波音频（模拟音调）"""
        num_samples = int(self.sample_rate * duration_seconds)
        t = np.linspace(0, duration_seconds, num_samples, False)

        # 生成正弦波
        wave_data = amplitude * np.sin(2 * np.pi * frequency * t)

        # 按照阿里云ASR要求转换为16位PCM
        # 先限制到[-1, 1]范围，防止溢出
        clipped_wave = np.clip(wave_data, -1.0, 1.0)

        # 按照标准Float32到Int16PCM转换
        audio_data = np.where(clipped_wave < 0,
                              clipped_wave * 32768,  # 0x8000 = 32768
                              # 0x7FFF = 32767
                              clipped_wave * 32767).astype(np.int16)

        return audio_data.tobytes()

    def generate_speech_like_audio(self, duration_seconds=3.0):
        """生成类似语音的复合音频"""
        num_samples = int(self.sample_rate * duration_seconds)
        t = np.linspace(0, duration_seconds, num_samples, False)

        # 基础频率（类似人声）
        base_freq = 150  # 男声基础频率约150Hz

        # 生成复合波形（模拟语音的复杂性）
        wave1 = 0.6 * np.sin(2 * np.pi * base_freq * t)
        wave2 = 0.3 * np.sin(2 * np.pi * base_freq * 2 * t)
        wave3 = 0.2 * np.sin(2 * np.pi * base_freq * 3 * t)

        # 添加调制（模拟语音的变化）
        modulation = 0.5 + 0.5 * np.sin(2 * np.pi * 5 * t)

        # 合成最终波形
        combined_wave = (wave1 + wave2 + wave3) * modulation

        # 添加一些随机噪声（模拟语音的自然性）
        noise = 0.05 * np.random.normal(0, 1, num_samples)
        combined_wave += noise

        # 应用包络（避免突然开始/结束）
        envelope = np.ones(num_samples)
        fade_samples = int(0.1 * self.sample_rate)  # 0.1秒淡入淡出
        envelope[:fade_samples] = np.linspace(0, 1, fade_samples)
        envelope[-fade_samples:] = np.linspace(1, 0, fade_samples)

        combined_wave *= envelope

        # 按照阿里云ASR要求转换为16位PCM
        # 先限制到[-1, 1]范围，防止溢出
        clipped_wave = np.clip(combined_wave, -1.0, 1.0)

        # 按照标准Float32到Int16PCM转换
        audio_data = np.where(clipped_wave < 0,
                              clipped_wave * 32768,  # 0x8000 = 32768
                              # 0x7FFF = 32767
                              clipped_wave * 32767).astype(np.int16)

        return audio_data.tobytes()

    def generate_chinese_greeting_simulation(self):
        """生成模拟中文问候的音频 - 增强版"""
        # 增加持续时间，让ASR有更多时间处理
        duration = 2.5  # 增加到2.5秒
        num_samples = int(self.sample_rate * duration)
        t = np.linspace(0, duration, num_samples, False)

        # 静音前缀（让ASR准备）
        silence_duration = 0.2
        silence_samples = int(self.sample_rate * silence_duration)

        # 第一个音节 "你" (ni) - 更明显的特征
        ni_start = silence_samples
        ni_duration = 0.8
        ni_samples = int(self.sample_rate * ni_duration)
        ni_t = t[ni_start:ni_start + ni_samples]
        ni_freq = 220  # 提高基础频率，更接近人声

        # 复合谐波，更像人声
        ni_wave = 0.6 * np.sin(2 * np.pi * ni_freq * (ni_t - ni_t[0]))
        ni_wave += 0.3 * np.sin(2 * np.pi * ni_freq * 2 * (ni_t - ni_t[0]))
        ni_wave += 0.15 * np.sin(2 * np.pi * ni_freq * 3 * (ni_t - ni_t[0]))
        ni_wave += 0.1 * np.sin(2 * np.pi * ni_freq * 4 * (ni_t - ni_t[0]))

        # 短暂停顿
        pause_duration = 0.1
        pause_samples = int(self.sample_rate * pause_duration)

        # 第二个音节 "好" (hao) - 音调变化更明显
        hao_start = ni_start + ni_samples + pause_samples
        hao_duration = 1.0
        hao_samples = int(self.sample_rate * hao_duration)
        hao_t = t[hao_start:hao_start + hao_samples]
        hao_t_norm = (hao_t - hao_t[0]) / hao_duration

        # 更复杂的音调变化（中文第三声特征）
        hao_freq_base = 200
        hao_freq = hao_freq_base * \
            (0.8 + 0.4 * np.sin(np.pi * hao_t_norm))  # 先降后升

        hao_wave = np.zeros_like(hao_t)
        for i, freq in enumerate(hao_freq):
            hao_wave[i] = 0.7 * \
                np.sin(2 * np.pi * freq * (hao_t[i] - hao_t[0]))
            hao_wave[i] += 0.3 * \
                np.sin(2 * np.pi * freq * 2 * (hao_t[i] - hao_t[0]))
            hao_wave[i] += 0.15 * \
                np.sin(2 * np.pi * freq * 3 * (hao_t[i] - hao_t[0]))

        # 合成完整音频
        full_wave = np.zeros(num_samples)

        # 添加第一个音节
        if ni_start + ni_samples <= num_samples:
            full_wave[ni_start:ni_start + ni_samples] = ni_wave

        # 添加第二个音节
        if hao_start + hao_samples <= num_samples:
            full_wave[hao_start:hao_start + hao_samples] = hao_wave
        else:
            available_samples = num_samples - hao_start
            full_wave[hao_start:] = hao_wave[:available_samples]

        # 更自然的包络
        envelope = np.ones(num_samples)

        # 整体淡入淡出
        fade_samples = int(0.1 * self.sample_rate)
        envelope[:fade_samples] = np.linspace(0, 1, fade_samples)
        envelope[-fade_samples:] = np.linspace(1, 0, fade_samples)

        # 为每个音节添加自然的强度变化
        for start, length in [(ni_start, ni_samples), (hao_start, min(hao_samples, num_samples - hao_start))]:
            if start + length <= num_samples:
                # 音节内的强度变化
                syllable_env = 0.3 + 0.7 * \
                    np.exp(-((np.arange(length) - length//2) / (length//4))**2)
                envelope[start:start + length] *= syllable_env

        full_wave *= envelope

        # 添加更真实的噪声和共振
        noise = 0.02 * np.random.normal(0, 1, num_samples)

        # 模拟声道共振
        resonance_freq = 800  # 常见的声道共振频率
        resonance = 0.1 * np.sin(2 * np.pi * resonance_freq * t)
        full_wave += noise + resonance

        # 按照阿里云ASR要求转换为16位PCM
        # 先限制到[-1, 1]范围，防止溢出
        clipped_wave = np.clip(full_wave, -1.0, 1.0)

        # 按照标准Float32到Int16PCM转换
        # sample < 0 ? sample * 0x8000 : sample * 0x7FFF
        audio_data = np.where(clipped_wave < 0,
                              clipped_wave * 32768,  # 0x8000 = 32768
                              # 0x7FFF = 32767
                              clipped_wave * 32767).astype(np.int16)

        return audio_data.tobytes()

    def generate_test_phrase(self, phrase="你好世界"):
        """生成测试短语的音频"""
        if phrase == "你好世界":
            return self.generate_enhanced_chinese_phrase()
        else:
            # 为其他短语生成基础音频
            return self.generate_speech_like_audio(duration=3.0)

    def generate_enhanced_chinese_phrase(self):
        """生成增强的中文短语'你好世界'"""
        duration = 3.5
        num_samples = int(self.sample_rate * duration)
        t = np.linspace(0, duration, num_samples, False)

        # 定义四个字的时间分配
        syllables = [
            {"start": 0.2, "duration": 0.7, "freq": 220, "tone": "rising"},      # 你
            {"start": 1.0, "duration": 0.8, "freq": 200,
                "tone": "falling_rising"},  # 好
            {"start": 2.0, "duration": 0.6, "freq": 180, "tone": "rising"},     # 世
            {"start": 2.8, "duration": 0.7, "freq": 160, "tone": "falling"}     # 界
        ]

        full_wave = np.zeros(num_samples)

        for syll in syllables:
            start_sample = int(syll["start"] * self.sample_rate)
            duration_samples = int(syll["duration"] * self.sample_rate)

            if start_sample + duration_samples > num_samples:
                duration_samples = num_samples - start_sample

            if duration_samples <= 0:
                continue

            syll_t = np.linspace(0, syll["duration"], duration_samples, False)
            base_freq = syll["freq"]

            # 根据声调生成频率变化
            if syll["tone"] == "rising":
                freq_mod = base_freq * (1.0 + 0.3 * syll_t / syll["duration"])
            elif syll["tone"] == "falling":
                freq_mod = base_freq * (1.0 - 0.3 * syll_t / syll["duration"])
            elif syll["tone"] == "falling_rising":
                # 先降后升（第三声）
                normalized_t = syll_t / syll["duration"]
                freq_mod = base_freq * \
                    (0.8 + 0.4 * np.sin(np.pi * normalized_t))
            else:
                freq_mod = base_freq

            # 生成复合波形
            wave = np.zeros_like(syll_t)
            for i, freq in enumerate(freq_mod):
                wave[i] = 0.6 * np.sin(2 * np.pi * freq * syll_t[i])
                wave[i] += 0.25 * np.sin(2 * np.pi * freq * 2 * syll_t[i])
                wave[i] += 0.15 * np.sin(2 * np.pi * freq * 3 * syll_t[i])

            # 音节包络
            envelope = np.exp(-((syll_t -
                              syll["duration"]/2) / (syll["duration"]/3))**2)
            wave *= envelope

            # 添加到完整波形
            full_wave[start_sample:start_sample + duration_samples] += wave

        # 全局处理
        envelope = np.ones(num_samples)
        fade_samples = int(0.1 * self.sample_rate)
        envelope[:fade_samples] = np.linspace(0, 1, fade_samples)
        envelope[-fade_samples:] = np.linspace(1, 0, fade_samples)

        full_wave *= envelope

        # 添加噪声和共振
        noise = 0.015 * np.random.normal(0, 1, num_samples)
        full_wave += noise

        # 按照阿里云ASR要求转换为16位PCM
        # 先限制到[-1, 1]范围，防止溢出
        clipped_wave = np.clip(full_wave, -1.0, 1.0)

        # 按照标准Float32到Int16PCM转换
        audio_data = np.where(clipped_wave < 0,
                              clipped_wave * 32768,  # 0x8000 = 32768
                              # 0x7FFF = 32767
                              clipped_wave * 32767).astype(np.int16)

        return audio_data.tobytes()

    def load_audio_file(self, file_path):
        """加载真实音频文件"""
        try:
            if not hasattr(self, '_loaded_audio_cache'):
                self._loaded_audio_cache = {}

            if file_path in self._loaded_audio_cache:
                return self._loaded_audio_cache[file_path]

            with wave.open(file_path, 'rb') as wav_file:
                frames = wav_file.readframes(wav_file.getnframes())

                # 检查音频格式
                if wav_file.getsampwidth() != 2:
                    print(f"⚠️ 音频文件位深度不是16位: {wav_file.getsampwidth() * 8}位")
                if wav_file.getframerate() != self.sample_rate:
                    print(
                        f"⚠️ 音频文件采样率不匹配: {wav_file.getframerate()}Hz (期望{self.sample_rate}Hz)")
                if wav_file.getnchannels() != self.channels:
                    print(
                        f"⚠️ 音频文件声道数不匹配: {wav_file.getnchannels()}声道 (期望{self.channels}声道)")

                # 缓存音频数据
                self._loaded_audio_cache[file_path] = frames
                print(f"✅ 已加载音频文件: {file_path} ({len(frames)} bytes)")
                return frames

        except Exception as e:
            print(f"❌ 加载音频文件失败 {file_path}: {e}")
            return None

    def chunk_audio_data(self, audio_data, chunk_size=None):
        """将音频数据分块"""
        if chunk_size is None:
            chunk_size = self.chunk_size * 2  # 2 bytes per sample for 16-bit

        chunks = []
        for i in range(0, len(audio_data), chunk_size):
            chunk = audio_data[i:i + chunk_size]
            chunks.append(chunk)

        return chunks


class UnifiedServiceClient:
    """统一服务客户端"""

    def __init__(self, server_url="ws://localhost:10004"):
        self.server_url = server_url
        self.websocket = None
        self.running = False

        # 音频配置
        self.audio_format = pyaudio.paInt16
        self.channels = 1
        self.sample_rate = 16000
        self.chunk_size = 1024

        # 音频组件
        self.pyaudio = None
        self.input_stream = None
        self.output_stream = None

        # 消息队列
        self.audio_output_queue = queue.Queue()

        # 虚拟音频生成器
        self.virtual_audio = VirtualAudioGenerator(
            sample_rate=self.sample_rate,
            channels=self.channels,
            chunk_size=self.chunk_size
        )

    async def connect(self):
        """连接到服务器"""
        try:
            self.websocket = await websockets.connect(self.server_url)
            print(f"✓ 已连接到服务器: {self.server_url}")
            return True
        except Exception as e:
            print(f"✗ 连接失败: {e}")
            return False

    async def disconnect(self):
        """断开连接"""
        self.running = False
        if self.websocket:
            await self.websocket.close()
            print("✓ 已断开连接")

    async def start_conversation(self, username="TestUser", asr_mode="ali"):
        """开始对话"""
        if not self.websocket:
            print("✗ 未连接到服务器")
            return False

        message = {
            "action": "start_conversation",
            "username": username,
            "asr_mode": asr_mode,
            "conversation_id": f"test_conv_{int(time.time())}"
        }

        await self.websocket.send(json.dumps(message))
        print(f"✓ 已发送开始对话请求: {username}, ASR模式: {asr_mode}")
        return True

    async def stop_conversation(self):
        """停止对话"""
        if not self.websocket:
            return False

        message = {"action": "stop_conversation"}
        await self.websocket.send(json.dumps(message))
        print("✓ 已发送停止对话请求")
        return True

    async def get_status(self):
        """获取状态"""
        if not self.websocket:
            return False

        message = {"action": "get_status"}
        await self.websocket.send(json.dumps(message))
        return True

    def init_audio(self):
        """初始化音频设备"""
        try:
            self.pyaudio = pyaudio.PyAudio()

            # 初始化输入流（麦克风）
            self.input_stream = self.pyaudio.open(
                format=self.audio_format,
                channels=self.channels,
                rate=self.sample_rate,
                input=True,
                frames_per_buffer=self.chunk_size
            )

            # 初始化输出流（扬声器）
            self.output_stream = self.pyaudio.open(
                format=self.audio_format,
                channels=self.channels,
                rate=self.sample_rate,
                output=True,
                frames_per_buffer=self.chunk_size
            )

            print("✓ 音频设备已初始化")
            return True

        except Exception as e:
            print(f"✗ 音频设备初始化失败: {e}")
            return False

    def cleanup_audio(self):
        """清理音频设备"""
        if self.input_stream:
            self.input_stream.stop_stream()
            self.input_stream.close()

        if self.output_stream:
            self.output_stream.stop_stream()
            self.output_stream.close()

        if self.pyaudio:
            self.pyaudio.terminate()

        print("✓ 音频设备已清理")

    async def send_audio_loop(self):
        """音频发送循环"""
        print("🎤 开始录音... (按Ctrl+C停止)")

        while self.running:
            try:
                # 读取音频数据
                audio_data = self.input_stream.read(
                    self.chunk_size, exception_on_overflow=False)

                # 发送到服务器
                if self.websocket:
                    await self.websocket.send(audio_data)

                await asyncio.sleep(0.01)  # 小延迟避免过载

            except Exception as e:
                print(f"音频发送出错: {e}")
                break

    async def send_virtual_audio(self, audio_type="speech", duration=3.0):
        """发送虚拟音频数据"""
        print(f"🎵 开始发送虚拟音频: {audio_type}, 时长: {duration}秒")

        try:
            # 生成虚拟音频数据
            if audio_type == "silence":
                audio_data = self.virtual_audio.generate_silence(duration)
                print("   生成静音数据")
            elif audio_type == "sine":
                audio_data = self.virtual_audio.generate_sine_wave(
                    440, duration, 0.3)
                print("   生成正弦波音频 (440Hz)")
            elif audio_type == "speech":
                audio_data = self.virtual_audio.generate_speech_like_audio(
                    duration)
                print("   生成类似语音的复合音频")
            elif audio_type == "greeting":
                audio_data = self.virtual_audio.generate_chinese_greeting_simulation()
                print("   生成增强版中文问候音频")
            elif audio_type == "phrase":
                audio_data = self.virtual_audio.generate_test_phrase("你好世界")
                print("   生成中文短语'你好世界'音频")
            else:
                print(f"❌ 未知的音频类型: {audio_type}")
                return

            # 将音频数据分块发送
            chunks = self.virtual_audio.chunk_audio_data(audio_data)
            print(f"   音频数据: {len(audio_data)} bytes, 分为 {len(chunks)} 个数据包")

            for i, chunk in enumerate(chunks):
                if not self.running or not self.websocket:
                    break

                await self.websocket.send(chunk)

                # 显示发送进度
                if i % 20 == 0:  # 每20个包显示一次
                    progress = (i + 1) / len(chunks) * 100
                    print(f"   📡 发送进度: {progress:.1f}% ({i+1}/{len(chunks)})")

                # 模拟实时发送
                await asyncio.sleep(0.02)  # 20ms间隔

            print("✅ 虚拟音频发送完成")

        except Exception as e:
            print(f"❌ 发送虚拟音频出错: {e}")

    async def send_audio_file_chunked(self, file_path):
        """分块发送音频文件"""
        try:
            audio_data = self.virtual_audio.load_audio_file(file_path)
            if not audio_data:
                return

            chunks = self.virtual_audio.chunk_audio_data(audio_data)
            print(f"📤 发送音频文件: {file_path}")
            print(f"   音频数据: {len(audio_data)} bytes, 分为 {len(chunks)} 个数据包")

            for i, chunk in enumerate(chunks):
                if not self.running or not self.websocket:
                    break

                await self.websocket.send(chunk)

                # 显示发送进度
                if i % 50 == 0:  # 每50个包显示一次
                    progress = (i + 1) / len(chunks) * 100
                    print(f"   📡 发送进度: {progress:.1f}% ({i+1}/{len(chunks)})")

                # 模拟实时发送
                await asyncio.sleep(0.02)

            print("✅ 音频文件发送完成")

        except Exception as e:
            print(f"❌ 发送音频文件出错: {e}")

    def audio_playback_worker(self):
        """音频播放工作线程"""
        while self.running:
            try:
                audio_data = self.audio_output_queue.get(timeout=1.0)
                if audio_data and self.output_stream:
                    self.output_stream.write(audio_data)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"音频播放出错: {e}")

    async def message_loop(self):
        """消息接收循环"""
        while self.running:
            try:
                message = await self.websocket.recv()

                if isinstance(message, str):
                    # JSON消息
                    data = json.loads(message)
                    await self.handle_text_message(data)

                elif isinstance(message, bytes):
                    # 音频数据
                    await self.handle_audio_message(message)

            except websockets.exceptions.ConnectionClosed:
                print("连接已关闭")
                break
            except Exception as e:
                print(f"消息接收出错: {e}")
                break

    async def handle_text_message(self, data: dict):
        """处理文本消息"""
        msg_type = data.get('type', 'unknown')

        if msg_type == 'asr_result':
            text = data.get('text', '')
            is_final = data.get('is_final', False)
            print(f"🎤 ASR识别: {text}")
            if is_final:
                print("   └─ [最终结果]")

        elif msg_type == 'llm_result':
            text = data.get('text', '')
            is_first = data.get('is_first', False)
            is_end = data.get('is_end', False)

            if is_first:
                print(f"🤖 LLM回复: {text}")
            else:
                print(f"   {text}")

            if is_end:
                print("   └─ [回复完成]")

        elif msg_type == 'audio_start':
            text = data.get('text', '')
            file_name = data.get('file_name', '')
            print(f"🔊 开始接收音频: '{text}' ({file_name})")
            # 重置音频接收缓冲
            self.current_audio_buffer = []

        elif msg_type == 'audio_end':
            text = data.get('text', '')
            total_size = data.get('total_size', 0)
            print(f"✅ 音频接收完成: '{text}' ({total_size} bytes)")

            # 可选：保存接收到的音频
            if hasattr(self, 'current_audio_buffer') and self.current_audio_buffer:
                await self.save_received_audio(text)

        elif msg_type == 'error':
            message = data.get('message', '')
            print(f"❌ 错误: {message}")

        elif data.get('status') == 'success':
            print(f"✓ {data.get('message', '操作成功')}")

        elif data.get('status') == 'error':
            print(f"✗ {data.get('message', '操作失败')}")

        else:
            print(f"📨 收到消息: {data}")

    async def handle_audio_message(self, audio_data: bytes):
        """处理音频消息"""
        # 存储接收到的音频数据
        if not hasattr(self, 'current_audio_buffer'):
            self.current_audio_buffer = []
        self.current_audio_buffer.append(audio_data)

        # 将音频数据放入播放队列
        self.audio_output_queue.put(audio_data)

        # 显示接收进度
        total_size = sum(len(chunk) for chunk in self.current_audio_buffer)
        if total_size > 0 and len(self.current_audio_buffer) % 10 == 0:  # 每10个chunk显示一次
            print(f"   📡 音频接收中: {total_size} bytes")

    async def save_received_audio(self, text: str):
        """保存接收到的音频到文件"""
        try:
            if not hasattr(self, 'current_audio_buffer') or not self.current_audio_buffer:
                return

            # 生成安全的文件名
            import os
            timestamp = int(time.time())
            safe_text = "".join(c for c in text if c.isalnum()
                                or c in (' ', '-', '_')).strip()[:20]
            filename = f"received_audio_{timestamp}_{safe_text}.wav"

            # 确保目录存在
            os.makedirs("test_audio", exist_ok=True)
            filepath = os.path.join("test_audio", filename)

            # 合并并保存音频数据
            audio_data = b''.join(self.current_audio_buffer)
            with open(filepath, 'wb') as f:
                f.write(audio_data)

            print(f"💾 音频已保存: {filepath} ({len(audio_data)} bytes)")

        except Exception as e:
            print(f"保存音频失败: {e}")

    async def send_text_directly(self, text="你好"):
        """直接发送文本给LLM，绕过ASR（用于测试LLM+TTS流程）"""
        try:
            print(f"📝 直接发送文本: '{text}'")

            # 构造模拟ASR结果
            mock_asr_result = {
                'type': 'asr_result',
                'text': text,
                'is_final': True,
                'timestamp': time.time()
            }

            # 显示模拟结果
            print(f"🎤 模拟ASR识别: {text} [最终结果]")

            # 发送给服务器（让服务器知道这是文本输入）
            text_message = {
                'action': 'text_input',
                'text': text,
                'timestamp': time.time()
            }

            await self.websocket.send(json.dumps(text_message))
            print("✅ 文本已发送到服务器")

        except Exception as e:
            print(f"❌ 发送文本失败: {e}")

    async def run_text_only_test(self, test_texts=None):
        """运行纯文本测试（绕过ASR，直接测试LLM+TTS）"""
        if not await self.connect():
            return

        if test_texts is None:
            test_texts = ["你好", "今天天气怎么样", "请介绍一下自己"]

        try:
            # 开始对话
            await self.start_conversation("TextOnlyUser", "ali")
            await asyncio.sleep(1)

            self.running = True

            # 启动消息接收
            message_task = asyncio.create_task(self.message_loop())

            # 发送文本消息
            for i, text in enumerate(test_texts):
                print(f"\n--- 测试 {i+1}/{len(test_texts)}: '{text}' ---")

                # 发送文本
                await self.send_text_directly(text)

                # 等待处理
                print("⏳ 等待LLM+TTS处理...")
                await asyncio.sleep(8)

                if i < len(test_texts) - 1:
                    print("📝 准备下一个测试...")
                    await asyncio.sleep(2)

            print("\n🎉 所有文本测试完成")

            # 停止对话
            await self.stop_conversation()
            await asyncio.sleep(2)

            self.running = False
            message_task.cancel()

        except Exception as e:
            print(f"文本测试出错: {e}")

        finally:
            await self.disconnect()

    async def run_interactive_mode(self):
        """运行交互模式"""
        if not await self.connect():
            return

        if not self.init_audio():
            return

        try:
            # 开始对话
            await self.start_conversation()

            # 等待服务器响应
            await asyncio.sleep(1)

            self.running = True

            # 启动音频播放线程
            playback_thread = threading.Thread(
                target=self.audio_playback_worker, daemon=True)
            playback_thread.start()

            # 启动并发任务
            await asyncio.gather(
                self.send_audio_loop(),
                self.message_loop()
            )

        except KeyboardInterrupt:
            print("\n收到中断信号，正在停止...")

        finally:
            await self.stop_conversation()
            await asyncio.sleep(1)
            self.cleanup_audio()
            await self.disconnect()

    async def run_virtual_audio_test(self, audio_types=None):
        """运行虚拟音频测试模式"""
        if not await self.connect():
            return

        if audio_types is None:
            audio_types = ["greeting", "phrase", "speech"]

        try:
            # 开始对话
            await self.start_conversation("VirtualTestUser", "ali")
            await asyncio.sleep(1)

            self.running = True

            # 启动消息接收
            message_task = asyncio.create_task(self.message_loop())

            # 发送不同类型的虚拟音频
            for i, audio_type in enumerate(audio_types):
                print(f"\n--- 测试 {i+1}/{len(audio_types)}: {audio_type} ---")

                # 发送虚拟音频
                await self.send_virtual_audio(audio_type, duration=2.0)

                # 等待处理
                print("⏳ 等待ASR+LLM+TTS处理...")
                await asyncio.sleep(8)  # 给足时间处理

                if i < len(audio_types) - 1:
                    print("📝 准备下一个测试...")
                    await asyncio.sleep(2)

            print("\n🎉 所有虚拟音频测试完成")

            # 停止对话
            await self.stop_conversation()
            await asyncio.sleep(2)

            self.running = False
            message_task.cancel()

        except Exception as e:
            print(f"虚拟音频测试出错: {e}")

        finally:
            await self.disconnect()

    async def run_file_test(self, audio_files=None):
        """运行音频文件测试模式"""
        if not await self.connect():
            return

        if audio_files is None:
            # 默认测试文件列表
            audio_files = [
                "test_audio/sample.wav",
                "cache_data/input.wav",
                "samples/test.wav"
            ]

        try:
            # 开始对话
            await self.start_conversation("FileTestUser", "ali")
            await asyncio.sleep(1)

            self.running = True

            # 启动消息接收
            message_task = asyncio.create_task(self.message_loop())

            # 发送音频文件
            file_sent = False
            for audio_file in audio_files:
                if os.path.exists(audio_file):
                    print(f"\n--- 测试音频文件: {audio_file} ---")
                    await self.send_audio_file_chunked(audio_file)
                    file_sent = True

                    # 等待处理
                    print("⏳ 等待ASR+LLM+TTS处理...")
                    await asyncio.sleep(10)
                    break

            if not file_sent:
                print("⚠️ 未找到测试音频文件，使用虚拟音频代替")
                await self.send_virtual_audio("greeting", duration=2.0)
                await asyncio.sleep(8)

            # 停止对话
            await self.stop_conversation()
            await asyncio.sleep(2)

            self.running = False
            message_task.cancel()

        except Exception as e:
            print(f"音频文件测试出错: {e}")

        finally:
            await self.disconnect()

    async def run_text_test(self):
        """运行简单文本测试模式（已废弃，使用虚拟音频测试）"""
        print("⚠️ 简单测试模式已更新为虚拟音频测试")
        await self.run_virtual_audio_test(["silence"])


def check_dependencies():
    """检查必要的依赖"""
    missing_deps = []

    try:
        import numpy
        print("✅ numpy 已安装")
    except ImportError:
        missing_deps.append("numpy")

    try:
        import pyaudio
        print("✅ pyaudio 已安装")
    except ImportError:
        missing_deps.append("pyaudio")

    if missing_deps:
        print(f"\n❌ 缺少依赖: {', '.join(missing_deps)}")
        print("请安装依赖：")
        for dep in missing_deps:
            if dep == "numpy":
                print("  pip install numpy")
            elif dep == "pyaudio":
                print("  pip install pyaudio")
        return False

    return True


async def main():
    """主函数"""
    print("=" * 60)
    print("        统一ASR+LLM+TTS服务客户端测试")
    print("=" * 60)

    # 检查依赖
    if not check_dependencies():
        return

    print("\n🎯 测试模式选择:")
    print("1. 交互模式 (需要麦克风和扬声器)")
    print("2. 虚拟音频测试 (推荐 - 无需硬件)")
    print("3. 音频文件测试")
    print("4. 增强中文问候测试 (更真实)")
    print("5. 中文短语测试 (你好世界)")
    print("6. 完整虚拟音频测试")
    print("7. 纯文本测试 (绕过ASR)")
    print("8. 调试模式 (详细日志)")
    print("请选择模式 (1-8): ", end="")

    try:
        choice = input().strip()
        client = UnifiedServiceClient()

        if choice == "1":
            print("\n🎙️ 启动交互模式...")
            print("需要麦克风和扬声器，确保音频设备正常工作")
            await client.run_interactive_mode()

        elif choice == "2":
            print("\n🎵 启动虚拟音频测试...")
            print("使用计算机生成的音频数据进行测试")
            await client.run_virtual_audio_test()

        elif choice == "3":
            print("\n📁 启动音频文件测试...")
            print("发送预录制的音频文件进行测试")
            await client.run_file_test()

        elif choice == "4":
            print("\n⚡ 增强中文问候测试...")
            print("使用增强版虚拟音频，更容易被ASR识别")
            await client.run_virtual_audio_test(["greeting"])

        elif choice == "5":
            print("\n📢 中文短语测试...")
            print("测试'你好世界'短语识别")
            await client.run_virtual_audio_test(["phrase"])

        elif choice == "6":
            print("\n🔬 完整虚拟音频测试...")
            print("测试所有类型的虚拟音频")
            await client.run_virtual_audio_test(["greeting", "phrase", "speech", "sine", "silence"])

        elif choice == "7":
            print("\n📝 纯文本测试...")
            print("绕过ASR直接测试LLM+TTS流程")
            await client.run_text_only_test()

        elif choice == "8":
            print("\n🐛 调试模式...")
            print("启用详细日志，测试增强问候音频")
            import logging
            logging.basicConfig(level=logging.DEBUG)
            await client.run_virtual_audio_test(["greeting"])

        else:
            print("❌ 无效选择，请输入1-8之间的数字")

    except KeyboardInterrupt:
        print("\n👋 程序已退出")
    except Exception as e:
        print(f"\n❌ 程序运行错误: {e}")


def print_usage_info():
    """打印使用信息"""
    print("\n📋 使用说明:")
    print("1. 确保统一服务已启动: python start_unified_service.py")
    print("2. 虚拟音频测试无需额外硬件，推荐用于功能验证")
    print("3. 交互模式需要麦克风和扬声器")
    print("4. 音频文件测试需要预先准备WAV格式音频文件")
    print("\n🔧 虚拟音频类型说明:")
    print("- greeting: 增强版中文问候('你好')音频，更真实")
    print("- phrase: 中文短语('你好世界')音频，带声调变化")
    print("- speech: 类似人声的复合音频")
    print("- sine: 纯音调正弦波")
    print("- silence: 静音数据")
    print("\n💡 推荐测试顺序:")
    print("1. 先用模式7测试纯文本输入（确保LLM+TTS工作）")
    print("2. 再用模式4测试增强问候音频")
    print("3. 然后用模式5测试短语识别")
    print("4. 如有问题用模式8进行调试")
    print("\n⚡ 快速验证:")
    print("- 模式7可以快速验证LLM和TTS是否正常工作")
    print("- 如果模式7成功但音频测试失败，说明是ASR识别问题")


if __name__ == "__main__":
    print_usage_info()
    asyncio.run(main())
