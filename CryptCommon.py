#coding=utf-8


import sys, os
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import base64


pad=lambda s:s+(16-len(s)%16)*chr(16-len(s)%16)
unpad=lambda s:s[0:-ord(s[-1])]


def add_to_16(text):
    ret = text + "0000000000000000"
    return ret[:16]


def encrypt(text, key):
    mode = AES.MODE_CBC
    secret_key = add_to_16(key)
    iv = secret_key
    cryptos = AES.new(secret_key, mode, iv)
    cipher_text = cryptos.encrypt(pad(text))
    ret = b2a_hex(cipher_text)
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret.upper()


def decrypt(text, key):
    mode = AES.MODE_CBC
    secret_key = add_to_16(key)
    iv = secret_key
    cryptos = AES.new(secret_key, mode, iv)
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
    ret = base64.b64encode(text.encode("utf-8"))
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret


def base64_decode(text):
    ret = base64.b64decode(text)
    if 3 == sys.version_info.major:
        ret = str(ret, "utf-8")
    return ret




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