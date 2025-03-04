import os

import os
import subprocess
import re
from tqdm import tqdm

import os
import subprocess
import re
from tqdm import tqdm

def parse_size(size_str):
    """
    将带单位的字符串（例如 '256.0 KiB'）转换为字节数。
    
    参数:
        size_str (str): 带单位的字符串
    返回:
        float: 字节数
    """
    units = {
        'B': 1,
        'KiB': 1024,
        'MiB': 1024**2,
        'GiB': 1024**3,
        'TiB': 1024**4,
    }
    match = re.match(r'(\d+\.?\d*)\s*(\w+)', size_str)
    if match:
        value, unit = match.groups()
        return float(value) * units.get(unit, 1)
    return 0

def upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url):
    """
    将本地文件上传到 Ceph 存储，并显示上传进度条。
    
    参数：
        filepath (str): 本地文件路径
        source_dir (str): 源目录（D:\）
        bucket_name (str): Ceph 桶名称（rfi_data）
        endpoint_url (str): Ceph 端点 URL
    """
    # 计算文件相对于源目录的路径，并转换为 S3 的键（使用 / 分隔符）
    relative_path = os.path.relpath(filepath, source_dir)
    key = relative_path.replace('\\', '/')
    
    # 构造上传命令，双引号处理路径中的空格
    upload_command = f'aws s3 cp "{filepath}" s3://{bucket_name}/{key} --endpoint-url={endpoint_url}'
    
    # 打印上传开始信息
    print(f"开始上传 {filepath} 到 s3://{bucket_name}/{key}")
    
    # 使用 subprocess.Popen 执行命令，捕获输出
    process = subprocess.Popen(upload_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # 获取文件大小并创建进度条
    file_size = os.path.getsize(filepath)
    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(filepath))
    
    # 实时读取 stdout 并解析进度
    for line in iter(process.stdout.readline, b''):
        line = line.decode().strip()
        match = re.search(r'Completed (\d+\.?\d*\s*\w+)/(\d+\.?\d*\s*\w+)', line)
        if match:
            uploaded_str, total_str = match.groups()
            uploaded = parse_size(uploaded_str)
            # 更新进度条，防止超出文件大小
            pbar.update(min(uploaded - pbar.n, file_size - pbar.n))
    
    # 关闭 stdout 并等待命令完成
    process.stdout.close()
    process.wait()
    pbar.close()
    
    # 检查上传结果
    if process.returncode == 0:
        print(f"成功上传 {os.path.basename(filepath)}")
    else:
        print(f"上传失败 {os.path.basename(filepath)}，返回码：{process.returncode}")

if __name__ == "__main__":
    # 配置参数
    source_dir = 'D:\\'  # 源目录（移动驱动器根目录）
    bucket_name = 'rfi'  # Ceph 存储桶名称
    endpoint_url = 'http://10.140.27.254:80'  # Ceph 端点 URL
    target_file = "A.fits"  # 开始上传的目标文件

    # 加载已上传文件集合
    if os.path.exists("uploaded_files.txt"):
        with open("uploaded_files.txt", "r", encoding="utf-8") as f:
            uploaded_files = set(line.strip() for line in f)
    else:
        uploaded_files = set()

    # 初始化用于收集 .fit 文件的列表
    fit_files = []
    start_collecting = False

    # 遍历目录，找到 target_file 并收集其及后续的 .fit 文件
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.fit'):
                filepath = os.path.join(root, file)
                # 检查是否为目标文件 "A.fits"
                if not start_collecting and file == target_file:
                    start_collecting = True
                # 一旦找到目标文件，开始收集文件
                if start_collecting:
                    fit_files.append(filepath)

    # 如果没有收集到文件，打印提示信息
    if not fit_files:
        print(f"在 {source_dir} 中未找到目标文件 {target_file}，没有文件需要上传。")
    else:
        # 遍历 fit_files 并上传文件，显示整体进度
        for filepath in tqdm(fit_files, desc="上传 .fit 文件"):
            if filepath not in uploaded_files:
                upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url, uploaded_files)
