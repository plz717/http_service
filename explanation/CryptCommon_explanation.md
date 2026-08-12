# CryptCommon.py 说明

## 用途

通用加解密工具模块：基于 **AES-CBC** 的加解密，以及 Base64 编解码。被 `EsManager.py` 用来解密 ES 密码；也可被 `es_lookup_program_id.py` 间接调用。

## 主要函数

| 函数 | 作用 |
|------|------|
| `encrypt(text, key)` | AES-CBC 加密，密钥/IV 取 key 补齐到 16 字节，输出大写十六进制字符串 |
| `decrypt(text, key)` | 对应解密 |
| `base64_encode(text)` | UTF-8 文本 → Base64 字符串 |
| `base64_decode(text)` | Base64 → 字符串 |
| `add_to_16` / `pad` / `unpad` | 密钥截断与 PKCS 风格填充辅助 |

## 使用方式

### 作为库导入

```python
import CryptCommon

secret_key = CryptCommon.base64_decode("WnRQS2xyZXcxZHlERnBWSA==")
plain = CryptCommon.decrypt("61e058ae3bc4d89e7c73763c875f57a9", secret_key)
cipher = CryptCommon.encrypt("明文", secret_key)
```

### 直接运行自测

```bash
python CryptCommon.py
```

会用内置样例明文/密钥打印加解密与 Base64 结果（仅本地自测，勿把真实密钥提交到公开环境）。

## 依赖

`pycrypto` / `pycryptodome`（`from Crypto.Cipher import AES`）。
