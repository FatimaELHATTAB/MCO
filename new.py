from ibm_boto3.s3.transfer import TransferConfig

bucket_name = "bu0021015718"

source_key = "IN/RMPM/NFC-SMG61_20260730.txt"
destination_key = "IN/RMPM/NFC-SMG61_20260730_replay.txt"

client = cos.meta.client

copy_source = {
    "Bucket": bucket_name,
    "Key": source_key
}

config = TransferConfig(
    multipart_threshold=512 * 1024 * 1024,  # 512 Mo
    multipart_chunksize=512 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True
)

client.copy(
    CopySource=copy_source,
    Bucket=bucket_name,
    Key=destination_key,
    Config=config
)

print("Copie terminée dans le COS")
