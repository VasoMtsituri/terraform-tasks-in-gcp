provider "google" {
      project = var.project
      region  = var.region
}

resource "google_composer_environment" "example_environment" {
  name = "example-environment"

  config {

    software_config {
      image_version = "composer-3-airflow-2.10.5-build.19"
    }

    node_config {
      service_account = "custom-service-account-524@central-catcher-471811-s3.iam.gserviceaccount.com"
    }
  }
}