import os

def upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url):
    """
    将本地文件上传到 Ceph 存储。
    
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
    
    # 执行上传命令
    ret = os.system(upload_command)
    
    # 检查上传结果
    if ret == 0:
        print(f"成功上传 {os.path.basename(filepath)}")
    else:
        print(f"上传失败 {os.path.basename(filepath)}，返回码：{ret}")

if __name__ == "__main__":
    # 配置参数
    source_dir = 'D:\\'  # 移动硬盘根目录
    bucket_name = 'rfi_data'  # Ceph 桶名称
    endpoint_url = 'http://10.140.31.252'  # Ceph 端点 URL
    
    # 遍历 D 盘，找到所有 .fit 文件并上传
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.fit'):
                filepath = os.path.join(root, file)
                upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url)