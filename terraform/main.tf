provider "google" {
      project = "central-catcher-471811-s3"
      region  = var.region
}

resource "google_storage_bucket" "function_source_bucket" {
      name          = "terraform-task-gcs-bucket-eu"
      location      = var.location
      force_destroy = true
}