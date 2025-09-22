from s3_file_downloader import make_s3_client, S3Downloader

def main():
    # Public access (no credentials needed)
    s3 = make_s3_client(region="us-east-1", unsigned=True)
    dl = S3Downloader(s3)

    bucket = "aws-bigdata-blog"
    prefix = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"

    # 1) List a few
    print("Listing all objects...")
    keys = dl.list_objects(bucket=bucket, prefix=prefix, limit=5)
    for k in keys:
        print(" -", k)

    # 2) Download ONE file
    if keys:
        dl.download_one(bucket=bucket, key=keys[0], dest_dir="./downloads_one")

    # 3) Download MANY (sequential)
    if keys:
        dl.download_many(bucket=bucket, keys=keys, dest_dir="./downloads_many")

    # 4) Download ALL under prefix (be careful: could be large!)
    # Use a small limit at first; remove 'limit' to fetch everything.
    dl.download_all_by_prefix(bucket=bucket, prefix=prefix, dest_dir="./downloads_all", limit=5)

if __name__ == "__main__":
    main()
