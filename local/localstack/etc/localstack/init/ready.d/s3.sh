#!/usr/bin/env bash
# vim: et sr sw=4 ts=4 smartindent syntax=bash:
set -e
echo "BUCKET_NAMES: $BUCKET_NAMES" > /var/tmp/foo
for bucket in $BUCKET_NAMES ; do
    echo "seeding bucket $bucket"
    content_dir="$BASE_CONTENT_DIR/$bucket"
    awslocal s3 mb "s3://$bucket"
    if [[ -d "$content_dir" ]]; then
        (
            cd "$content_dir"
            aws s3 cp . "s3://$bucket" --recursive
        )
    fi
done
