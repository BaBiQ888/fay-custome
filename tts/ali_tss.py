import http.client
import urllib.parse
import json
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from core.authorize_tb import Authorize_Tb
import time
from utils import util, config_util
from utils import config_util as cfg
import wave
import subprocess
import os


class Speech:
    def __init__(self):
        self.key_ali_nls_key_id = cfg.key_ali_tss_key_id
        self.key_ali_nls_key_secret = cfg.key_ali_tss_key_secret
        self.ali_nls_app_key = cfg.key_ali_tss_app_key
        self.token = None
        self.authorize_tb = Authorize_Tb()
        self.__history_data = []

        # 🔧 新增：确保必要的目录存在
        self._ensure_directories()

    def _ensure_directories(self):
        """确保必要的目录存在"""
        try:
            # 创建samples目录
            samples_dir = './samples'
            if not os.path.exists(samples_dir):
                os.makedirs(samples_dir)
                print(f"✅ 创建目录: {samples_dir}")

            # 创建temp目录
            temp_dir = './temp'
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
                print(f"✅ 创建目录: {temp_dir}")

        except Exception as e:
            print(f"⚠️ 创建目录失败: {e}")

    def connect(self):
        pass

    def __get_history(self, voice_name, style, text):
        for data in self.__history_data:
            if data[0] == voice_name and data[1] == style and data[2] == text:
                return data[3]
        return None

    def set_token(self):
        # 🔧 新增：配置验证
        if not self._validate_config():
            print("❌ TTS配置验证失败，请检查配置")
            return

        token = self.__check_token()
        if token is None or token == 'expired':
            token_info = self.__get_token()
            if token_info is not None and token_info['Id'] is not None:
                expires_timedelta = token_info['ExpireTime']
                expiry_timestamp_in_milliseconds = expires_timedelta * 1000
                if token == 'expired':
                    self.authorize_tb.update_by_userid(
                        self.key_ali_nls_key_id, token_info['Id'], expiry_timestamp_in_milliseconds)
                else:
                    self.authorize_tb.add(
                        self.key_ali_nls_key_id, token_info['Id'], expiry_timestamp_in_milliseconds)
                token = token_info['Id']
            else:
                print(f"请检查阿里云tts对接")
                token = None

        self.token = token

    def _validate_config(self) -> bool:
        """验证TTS配置是否完整"""
        try:
            if not self.key_ali_nls_key_id or not self.key_ali_nls_key_id.strip():
                print("❌ 阿里云TTS Key ID未配置")
                return False

            if not self.key_ali_nls_key_secret or not self.key_ali_nls_key_secret.strip():
                print("❌ 阿里云TTS Key Secret未配置")
                return False

            if not self.ali_nls_app_key or not self.ali_nls_app_key.strip():
                print("❌ 阿里云TTS App Key未配置")
                return False

            print("✅ TTS配置验证通过")
            return True

        except Exception as e:
            print(f"❌ TTS配置验证异常: {e}")
            return False

    def __check_token(self):
        self.authorize_tb.init_tb()
        info = self.authorize_tb.find_by_userid(self.key_ali_nls_key_id)
        if info is not None:
            if info[1] >= int(time.time())*1000:
                return info[0]
            else:
                return 'expired'
        else:
            return None

    def __get_token(self):
        try:
            global _token
            __client = AcsClient(
                self.key_ali_nls_key_id,
                self.key_ali_nls_key_secret,
                "cn-shanghai"
            )

            __request = CommonRequest()
            __request.set_method('POST')
            __request.set_domain('nls-meta.cn-shanghai.aliyuncs.com')
            __request.set_version('2019-02-28')
            __request.set_action_name('CreateToken')
            info = json.loads(__client.do_action_with_exception(__request))
            _token = info['Token']
            return info['Token']
        except Exception as e:
            print(f"阿里云tts对接有误： {str(e)}")
            return None

    def to_sample(self, text, style):
        file_url = None
        try:
            history = self.__get_history(config_util.config["attribute"]["voice"] if config_util.config["attribute"]
                                         ["voice"] is not None and config_util.config["attribute"]["voice"].strip() != "" else "知小夏", style, text)
            if history is not None:
                return history
            self.set_token()
            if self.token != None:
                host = 'nls-gateway-cn-shanghai.aliyuncs.com'
                url = 'https://' + host + '/stream/v1/tts'
                # 设置HTTPS Headers。
                httpHeaders = {
                    'Content-Type': 'application/json'
                }
                # text = f"<speak>{text}</speak>"
                # 设置HTTPS Body。
                # 🔧 修复：改为WAV格式，使用标准采样率
                body = {
                    'appkey': self.ali_nls_app_key,
                    'token': self.token,
                    'speech_rate': 0,
                    'text': text,
                    'format': 'wav',           # ✅ 改为WAV格式（无损）
                    'sample_rate': 44100,      # ✅ 使用标准采样率44.1kHz
                    'voice': 'zhixiaomei',
                    'bit_rate': 16,            # ✅ 添加位深度
                    'channels': 1              # ✅ 明确声道数
                }
                body = json.dumps(body)
                conn = http.client.HTTPSConnection(host)
                conn.request(method='POST', url=url,
                             body=body, headers=httpHeaders)
                # 处理服务端返回的响应。
                response = conn.getresponse()
                tt = time.time()
                contentType = response.getheader('Content-Type')
                body = response.read()

                # 🔧 修复：处理WAV格式响应
                if 'audio/wav' == contentType or 'audio/x-wav' == contentType:
                    # WAV格式处理
                    file_url = './samples/sample-' + \
                        str(int(time.time() * 1000)) + '.wav'

                    # 直接保存WAV数据
                    with open(file_url, 'wb') as f:
                        f.write(body)

                    # 🔧 新增：音频质量检查
                    quality_check = self._check_audio_quality(file_url)
                    if quality_check['is_valid']:
                        util.log(
                            1, f"[✅] TTS生成成功: {file_url}, 质量: {quality_check}")
                    else:
                        util.log(
                            1, f"[⚠️] 音频质量检查失败: {quality_check['reason']}")
                        # 可以尝试重新生成或使用备用方案

                elif 'audio/mpeg' == contentType:
                    # 🔧 兼容：如果返回MP3，转换为WAV
                    util.log(1, "[⚠️] 服务端返回MP3格式，进行转换...")

                    temp_mp3 = './temp/sample-' + \
                        str(int(time.time() * 1000)) + '.mp3'
                    wav_file = './samples/sample-' + \
                        str(int(time.time() * 1000)) + '.wav'

                    # 保存临时MP3
                    with open(temp_mp3, 'wb') as f:
                        f.write(body)

                    # 转换为高质量WAV
                    try:
                        subprocess.run([
                            'ffmpeg', '-i', temp_mp3,
                            '-ar', '44100',        # 44.1kHz采样率
                            '-ac', '1',            # 单声道
                            '-acodec', 'pcm_s16le',  # 16位PCM编码
                            '-f', 'wav',
                            wav_file, '-y'
                        ], check=True, capture_output=True)

                        # 清理临时文件
                        os.remove(temp_mp3)
                        file_url = wav_file

                        util.log(1, f"[✅] MP3转WAV成功: {wav_file}")

                    except Exception as e:
                        util.log(1, f"[⚠️] 格式转换失败，使用原始MP3: {e}")
                        # 如果转换失败，重命名MP3文件
                        os.rename(temp_mp3, wav_file.replace('.wav', '.mp3'))
                        file_url = wav_file.replace('.wav', '.mp3')

                else:
                    util.log(1, "[x] 语音转换失败！")
                    util.log(1, "[x] 原因: " + str(body))
                    util.log(1, f"[x] Content-Type: {contentType}")
                    file_url = None
                    return file_url
                conn.close()
                return file_url
            else:
                util.log(1, "[x] 语音转换失败！")
                util.log(1, "[x] 原因: 对接有误")
                file_url = None
                return file_url
        except Exception as e:
            util.log(1, "[x] 语音转换失败！")
            util.log(1, "[x] 原因: " + str(str(e)))
            file_url = None
            return file_url

    # 🔧 新增：音频质量检查方法
    def _check_audio_quality(self, audio_file: str) -> dict:
        """检查音频文件质量"""
        try:
            if not os.path.exists(audio_file):
                return {'is_valid': False, 'reason': '文件不存在'}

            file_size = os.path.getsize(audio_file)
            if file_size < 1024:  # 小于1KB
                return {'is_valid': False, 'reason': '文件过小'}

            # 检查文件头
            with open(audio_file, 'rb') as f:
                header = f.read(16)

            if audio_file.endswith('.wav'):
                # 检查WAV文件头
                if header[:4] != b'RIFF' or header[8:12] != b'WAVE':
                    return {'is_valid': False, 'reason': 'WAV文件头无效'}
            elif audio_file.endswith('.mp3'):
                # 检查MP3文件头
                if not (header[0] == 0xFF and (header[1] & 0xE0) == 0xE0):
                    return {'is_valid': False, 'reason': 'MP3文件头无效'}

            return {
                'is_valid': True,
                'file_size': file_size,
                'format': os.path.splitext(audio_file)[1],
                'reason': '质量检查通过'
            }

        except Exception as e:
            return {'is_valid': False, 'reason': f'检查异常: {str(e)}'}

    def close(self):
        pass
