#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
纯虚拟音频测试脚本
无需任何硬件设备，使用numpy生成虚拟音频数据测试统一服务
适用于CI/CD环境和无音频设备的服务器测试
"""

import asyncio
import websockets
import json
import time
import numpy as np


class SimpleVirtualAudioGenerator:
    """简化的虚拟音频生成器（无需pyaudio）"""

    def __init__(self, sample_rate=16000):
        self.sample_rate = sample_rate

    def generate_chinese_greeting(self):
        """生成模拟"你好"的音频信号"""
        duration = 1.5  # 1.5秒
        num_samples = int(self.sample_rate * duration)
        t = np.linspace(0, duration, num_samples, False)

        # 模拟中文"你好"的音调特征
        # 第一音节"你"：较高频率
        freq1 = 200
        part1_end = int(num_samples * 0.6)
        wave1 = 0.8 * np.sin(2 * np.pi * freq1 * t[:part1_end])

        # 第二音节"好"：频率变化
        freq2_base = 180
        part2_t = t[part1_end:]
        freq2 = freq2_base + 30 * np.sin(2 * np.pi * 3 * part2_t)
        wave2 = 0.7 * np.sin(2 * np.pi * freq2 * part2_t)

        # 合并音频
        full_wave = np.zeros(num_samples)
        full_wave[:part1_end] = wave1
        full_wave[part1_end:] = wave2

        # 添加包络和轻微噪声
        envelope = np.ones(num_samples)
        fade_len = int(0.1 * self.sample_rate)
        envelope[:fade_len] = np.linspace(0, 1, fade_len)
        envelope[-fade_len:] = np.linspace(1, 0, fade_len)

        full_wave *= envelope
        noise = 0.02 * np.random.normal(0, 1, num_samples)
        full_wave += noise

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


async def test_unified_service():
    """测试统一服务"""
    print("🚀 开始虚拟音频测试...")

    # 创建音频生成器
    audio_gen = SimpleVirtualAudioGenerator()

    try:
        # 连接服务器
        print("🔌 连接服务器...")
        websocket = await websockets.connect("ws://localhost:10004")
        print("✅ 连接成功")

        # 开始对话
        print("📞 开始对话...")
        start_msg = {
            "action": "start_conversation",
            "username": "VirtualTestUser",
            "asr_mode": "ali"
        }
        await websocket.send(json.dumps(start_msg))

        # 等待服务器响应
        response = await websocket.recv()
        print(f"📨 服务器响应: {response}")

        # 启动消息监听
        async def listen_responses():
            try:
                while True:
                    message = await websocket.recv()
                    if isinstance(message, str):
                        data = json.loads(message)
                        msg_type = data.get('type', '')

                        if msg_type == 'asr_result':
                            text = data.get('text', '')
                            is_final = data.get('is_final', False)
                            print(f"🎤 ASR: {text}" +
                                  (" [最终]" if is_final else ""))

                        elif msg_type == 'llm_result':
                            text = data.get('text', '')
                            is_first = data.get('is_first', False)
                            print(f"🤖 LLM: {text}" +
                                  (" [开始]" if is_first else ""))

                        elif msg_type == 'audio_start':
                            text = data.get('text', '')
                            print(f"🔊 TTS开始: '{text}'")

                        elif msg_type == 'audio_end':
                            text = data.get('text', '')
                            size = data.get('total_size', 0)
                            print(f"✅ TTS完成: '{text}' ({size} bytes)")

                        elif msg_type == 'error':
                            print(f"❌ 错误: {data.get('message', '')}")
                        else:
                            print(f"📨 消息: {data}")
                    elif isinstance(message, bytes):
                        # 接收到音频数据（可以保存或播放）
                        pass

            except websockets.exceptions.ConnectionClosed:
                print("🔌 连接已关闭")

        # 启动监听任务
        listen_task = asyncio.create_task(listen_responses())

        # 生成并发送虚拟音频
        print("🎵 生成虚拟音频数据...")
        audio_data = audio_gen.generate_chinese_greeting()
        print(f"   生成音频: {len(audio_data)} bytes")

        # 分块发送音频数据
        chunk_size = 2048  # 1024 samples * 2 bytes
        chunks = [audio_data[i:i+chunk_size]
                  for i in range(0, len(audio_data), chunk_size)]

        print(f"📤 发送音频数据 ({len(chunks)} 个数据包)...")
        for i, chunk in enumerate(chunks):
            await websocket.send(chunk)
            if i % 10 == 0:
                progress = (i + 1) / len(chunks) * 100
                print(f"   发送进度: {progress:.1f}%")
            await asyncio.sleep(0.02)  # 模拟实时发送

        print("✅ 音频发送完成")

        # 等待处理
        print("⏳ 等待ASR+LLM+TTS处理...")
        await asyncio.sleep(10)

        # 停止对话
        print("⏹️ 停止对话...")
        stop_msg = {"action": "stop_conversation"}
        await websocket.send(json.dumps(stop_msg))

        await asyncio.sleep(2)
        listen_task.cancel()
        await websocket.close()

        print("🎉 测试完成!")

    except ConnectionRefusedError:
        print("❌ 无法连接到服务器，请确保统一服务已启动:")
        print("   python start_unified_service.py")
    except Exception as e:
        print(f"❌ 测试出错: {e}")


def check_numpy():
    """检查numpy是否可用"""
    try:
        import numpy
        print("✅ numpy 可用")
        return True
    except ImportError:
        print("❌ 需要安装numpy: pip install numpy")
        return False


async def main():
    """主函数"""
    print("=" * 50)
    print("    纯虚拟音频测试")
    print("=" * 50)

    if not check_numpy():
        return

    print("\n🎯 此测试将:")
    print("1. 生成模拟'你好'的虚拟音频")
    print("2. 通过WebSocket发送给统一服务")
    print("3. 测试ASR+LLM+TTS完整流程")
    print("4. 无需任何音频硬件设备")

    print("\n按Enter开始测试，Ctrl+C退出...")
    try:
        input()
        await test_unified_service()
    except KeyboardInterrupt:
        print("\n👋 测试已取消")


if __name__ == "__main__":
    asyncio.run(main())
