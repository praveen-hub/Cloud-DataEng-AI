provider "aws" {
  access_key = "AKIAYSOTVPILQIT3YU6J"
  secret_key = "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"
  region     = "ap-south-1"
}
terraform {
	backend "s3" {
	bucket = "mpl-state"
	region     = "ap-south-1"
	}
	
