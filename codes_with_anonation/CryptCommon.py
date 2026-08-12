#coding=utf-8
# =============================================================================
# 【文件说明】CryptCommon.py（带中文注释的副本，原文件未改动）
# -----------------------------------------------------------------------------
# 用途：通用加解密工具模块。
#   - AES-CBC 加解密（密钥/IV 由 key 补齐到 16 字节）
#   - Base64 编解码
# 主要被 EsManager.py 用来解密 ES 密码；es_lookup_program_id.py 也会间接调用。
#
# 依赖：pycrypto / pycryptodome（from Crypto.Cipher import AES）
#
# 使用示例：
#   import CryptCommon
#   key = CryptCommon.base64_decode("...")
#   plain = CryptCommon.decrypt(cipher_hex, key)
#
# 直接运行本文件会执行底部自测（打印加解密与 Base64 结果）。
# =============================================================================


import sys, os
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import base64


# PKCS#7 风格填充：补齐到 16 字节的整数倍，填充字节值为「缺少的字节数」
pad=lambda s:s+(16-len(s)%16)*chr(16-len(s)%16)
# 去掉末尾填充：最后一个字节的数值表示填充长度
unpad=lambda s:s[0:-ord(s[-1])]


def add_to_16(text):
    """
    将密钥字符串处理成恰好 16 字节。
    做法：先在末尾拼接 16 个 '0'，再截取前 16 位。
    AES-128 要求 key / IV 长度为 16。
    """
    ret = text + "0000000000000000"
    return ret[:16]


def encrypt(text, key):
    """
    AES-CBC 加密。
    参数：
      text - 明文（str）
      key  - 密钥字符串（会经 add_to_16 处理；IV 与 key 相同）
    返回：
      大写十六进制密文字符串
    """
    mode = AES.MODE_CBC
    secret_key = add_to_16(key)
    iv = secret_key  # 本实现里 IV 直接复用密钥，非随机 IV
    cryptos = AES.new(secret_key, mode, iv)
    cipher_text = cryptos.encrypt(pad(text))
    # 二进制密文转十六进制
    ret = b2a_hex(cipher_text)
    if 3 == sys.version_info.major:
        # Python3 下 b2a_hex 返回 bytes，需转成 str
        ret = str(ret, "utf-8")
    return ret.upper()


def decrypt(text, key):
    """
    AES-CBC 解密（与 encrypt 对应）。
    参数：
      text - 十六进制密文字符串
      key  - 与加密时相同的密钥
    返回：
      明文字符串（Python3 下为 str）
    """
    mode = AES.MODE_CBC
    secret_key = add_to_16(key)
    iv = secret_key
    cryptos = AES.new(secret_key, mode, iv)
    # 先把十六进制还原为二进制，再解密
    plain_text = cryptos.decrypt(a2b_hex(text))
    if 3 == sys.version_info.major:
        plain_text = str(plain_text, "utf-8")
        plain_text = bytes(unpad(plain_text), "utf-8")
    else:
        plain_text = unpad(plain_text)
    ret = bytes.decode(plain_text).encode("utf-8")
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret


def base64_encode(text):
    """
    对 UTF-8 文本做 Base64 编码，返回字符串。
    """
    ret = base64.b64encode(text.encode("utf-8"))
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret


def base64_decode(text):
    """
    Base64 解码为字符串（Python3）。
    EsManager.password() 用它把 SECRET 还原成 AES 密钥。
    """
    ret = base64.b64decode(text)
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret



# ---------------------------------------------------------------------------
# 本地自测入口：演示 encrypt / decrypt / base64 往返
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    text = "KvYDn8*8bHpM"
    secret_key = "Nk1NGjh33dnaMhtS"


    e = encrypt(text, secret_key)
    print("encrypt:")
    print("%s\t%s" %(e, type(e)))
    d = decrypt(e, secret_key)
    print("decrypt:")
    print("%s\t%s" %(d, type(d)))


    print("base64 encode")
    s_base64 = base64_encode(secret_key)
    print("%s\t%s" %(s_base64, type(s_base64)))
    print("base64 decode")
    s = base64_decode(s_base64)
    print("%s\t%s" %(s, type(s)))
