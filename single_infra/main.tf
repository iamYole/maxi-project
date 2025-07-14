resource "google_storage_bucket" "sales_bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = var.force_destroy

  lifecycle_rule {
    condition {
      age = var.lifecycle_rule_age
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}