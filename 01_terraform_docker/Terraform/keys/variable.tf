variable "credentials" {
  description = "My Credentials"
  default     = "./my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "molten-rex-448411-s9"
}

variable "region" {
  description = "Region"
  default     = "africa-south1"
}

variable "location" {
  description = "Project Location"
  default     = "africa-south1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Name"
  default     = "molten-rex-448411-s9-terra-bucket"
}