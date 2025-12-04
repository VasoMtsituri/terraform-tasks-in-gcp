variable "project" {
  description = "GCP project ID"
  type        = string
  default     = "central-catcher-471811-s3"
}

variable "location" {
  description = "Location of GCP products"
  type        = string
  default     = "EU"
}

variable "region" {
  description = "Region for deployment"
  type        = string
  default     = "europe-central2"
}
