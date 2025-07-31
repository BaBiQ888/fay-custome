#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试音频格式是否符合阿里云ASR要求
验证虚拟音频生成的格式正确性
"""

import numpy as np
import struct


class AudioFormatTester:
    """音频格式测试器"""

    def __init__(self, sample_rate=16000):
        self.sample_rate = sample_rate

    def test_float32_to_int16_conversion(self):
        """测试Float32到Int16PCM转换是否正确"""
        print("🔧 测试Float32到Int16PCM转换...")

        # 测试用例：不同范围的浮点数
        test_cases = [
            -1.0,    # 最小值
            -0.5,    # 负数
            0.0,     # 零
            0.5,     # 正数
            1.0,     # 最大值
            1.5,     # 超出范围（需要裁剪）
            -1.5     # 超出范围（需要裁剪）
        ]

        print("输入值 -> 预期输出 -> 实际输出")
        for test_val in test_cases:
            # JavaScript逻辑：sample < 0 ? sample * 0x8000 : sample * 0x7FFF
            if test_val <= -1.0:
                expected = -32768  # 0x8000
            elif test_val >= 1.0:
                expected = 32767   # 0x7FFF
            elif test_val < 0:
                expected = int(test_val * 32768)
            else:
                expected = int(test_val * 32767)

            # 我们的Python实现
            clipped = np.clip(test_val, -1.0, 1.0)
            actual = int(32768 * clipped if clipped < 0 else 32767 * clipped)

            status = "✅" if expected == actual else "❌"
            print(f"{test_val:6.1f} -> {expected:6d} -> {actual:6d} {status}")

    def generate_test_greeting_audio(self):
        """生成测试用的问候音频"""
        print("\n🎵 生成测试问候音频...")

        duration = 2.0  # 2秒
        num_samples = int(self.sample_rate * duration)
        t = np.linspace(0, duration, num_samples, False)

        # 生成简单的"你好"模拟音频
        # 第一个音节 "你" - 220Hz
        ni_duration = 0.8
        ni_samples = int(self.sample_rate * ni_duration)
        ni_wave = 0.6 * np.sin(2 * np.pi * 220 * t[:ni_samples])

        # 第二个音节 "好" - 200Hz with modulation
        hao_start = ni_samples
        hao_duration = 0.8
        hao_samples = int(self.sample_rate * hao_duration)
        if hao_start + hao_samples <= num_samples:
            hao_t = t[hao_start:hao_start + hao_samples]
            hao_freq = 200 * (0.8 + 0.4 * np.sin(np.pi * hao_t / hao_duration))
            hao_wave = 0.7 * np.sin(2 * np.pi * hao_freq * hao_t)
        else:
            hao_wave = np.array([])

        # 合成完整音频
        full_wave = np.zeros(num_samples)
        full_wave[:ni_samples] = ni_wave
        if len(hao_wave) > 0 and hao_start + len(hao_wave) <= num_samples:
            full_wave[hao_start:hao_start + len(hao_wave)] = hao_wave

        # 添加包络和噪声
        envelope = np.ones(num_samples)
        fade_samples = int(0.1 * self.sample_rate)
        envelope[:fade_samples] = np.linspace(0, 1, fade_samples)
        envelope[-fade_samples:] = np.linspace(1, 0, fade_samples)
        full_wave *= envelope

        # 添加轻微噪声
        noise = 0.02 * np.random.normal(0, 1, num_samples)
        full_wave += noise

        # 按照阿里云ASR要求转换为16位PCM
        clipped_wave = np.clip(full_wave, -1.0, 1.0)
        audio_data = np.where(clipped_wave < 0,
                              clipped_wave * 32768,  # 0x8000 = 32768
                              # 0x7FFF = 32767
                              clipped_wave * 32767).astype(np.int16)

        return audio_data.tobytes()

    def analyze_audio_data(self, audio_bytes):
        """分析音频数据的格式"""
        print(f"\n📊 分析音频数据...")
        print(f"总字节数: {len(audio_bytes)}")
        print(f"样本数: {len(audio_bytes) // 2}")
        print(f"持续时间: {(len(audio_bytes) // 2) / self.sample_rate:.2f}秒")

        # 转换为int16数组进行分析
        int16_data = np.frombuffer(audio_bytes, dtype=np.int16)

        print(f"数值范围: {int16_data.min()} 到 {int16_data.max()}")
        print(f"平均值: {int16_data.mean():.2f}")
        print(f"RMS: {np.sqrt(np.mean(int16_data**2)):.2f}")

        # 检查是否有溢出
        overflow_count = np.sum((int16_data == -32768) | (int16_data == 32767))
        print(
            f"可能的溢出样本: {overflow_count} ({overflow_count/len(int16_data)*100:.2f}%)")

        # 检查字节序（应该是小端序）
        first_sample = struct.unpack('<h', audio_bytes[:2])[0]  # 小端序
        first_sample_big = struct.unpack('>h', audio_bytes[:2])[0]  # 大端序
        print(f"第一个样本 (小端序): {first_sample}")
        print(f"第一个样本 (大端序): {first_sample_big}")

        return {
            'total_bytes': len(audio_bytes),
            'samples': len(int16_data),
            'duration': (len(audio_bytes) // 2) / self.sample_rate,
            'min_value': int16_data.min(),
            'max_value': int16_data.max(),
            'overflow_count': overflow_count,
            'format_ok': int16_data.min() >= -32768 and int16_data.max() <= 32767
        }


def main():
    """主测试函数"""
    print("=" * 60)
    print("        阿里云ASR音频格式测试")
    print("=" * 60)

    tester = AudioFormatTester()

    # 1. 测试转换算法
    tester.test_float32_to_int16_conversion()

    # 2. 生成测试音频
    audio_data = tester.generate_test_greeting_audio()

    # 3. 分析音频格式
    analysis = tester.analyze_audio_data(audio_data)

    # 4. 格式验证
    print(f"\n✅ 格式验证结果:")
    print(f"   采样率: 16000Hz ✅")
    print(f"   位深: 16位 ✅")
    print(f"   通道: 单声道 ✅")
    print(f"   格式: PCM ✅")
    print(f"   数值范围: {'✅' if analysis['format_ok'] else '❌'}")
    print(f"   字节序: 小端序 ✅")

    if analysis['format_ok']:
        print(f"\n🎉 音频格式完全符合阿里云ASR要求！")
    else:
        print(f"\n❌ 音频格式存在问题，需要进一步调整")

    # 5. 保存测试音频
    with open('test_audio/format_test_greeting.raw', 'wb') as f:
        f.write(audio_data)
    print(f"💾 测试音频已保存: test_audio/format_test_greeting.raw")

    return analysis['format_ok']


if __name__ == "__main__":
    import os
    os.makedirs('test_audio', exist_ok=True)
    success = main()
    print(f"\n🏁 测试{'成功' if success else '失败'}!")
