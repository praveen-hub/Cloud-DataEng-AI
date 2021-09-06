provider "aws" {
  access_key = "AKIAYSOTVPILQIT3YU6J"
  secret_key = "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"
  region     = "ap-south-1"
}

resource "aws_s3_bucket" "bucket" {
  bucket = "my-tf-testl-bucktypm"
  acl    = "private"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}
