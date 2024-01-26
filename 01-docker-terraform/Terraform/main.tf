terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  project     = var.project_name
  region      = "us-central1"
}



resource "google_storage_bucket" "tlii-bucket" {
  name          = var.gcs_bucket_name
  location      = "US"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "tlii-dataset" {
  dataset_id = var.bq_dataset_name
}