from minio import Minio
from minio.error import S3Error
def minio_upload():
    # Create a client with the MinIO server playground, its access key and secret key.
    client = Minio(
        "play.min.io",
        access_key="Q3AM3UQ867SPQQA43P2F",
        secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    )


    found = client.bucket_exists("motion-detection")
    if not found:
        client.make_bucket("motion-detection")
    else:
        print("Bucket 'motion-detection' already exists")


    client.fput_object(
        "motion-detection", "motion-detection.avi", "./motiondetect.avi",num_parallel_uploads = 4
    )
    print(
        "motion-detection.avi has been uploaded to cloud"
    )

try:
     minio_upload()
except S3Error as exc:
    print("error occurred.", exc)