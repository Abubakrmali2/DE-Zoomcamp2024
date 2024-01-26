variable "Credential" {
  description = "My Credential to GCP"
  default     = "/workspaces/DE-Zoomcamp2024/Terraform/Keys/my-creds.json"
}

variable "project_name" {
  description = "my project name"
  default     = "mydezoomcamp-project"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "tlii_dataset"
}

variable "gcs_bucket_name" {
  description = "bucket name"
  default     = "mydezoomcamp-project-tlii-bucket"
}

variable "gcs_storage_class" {
  description = "bucket storage class"
  default     = "STANDARD"
}