variable "bucket_name" {
  type = string
}
variable "location" {
  type = string
}
variable "force_destroy" {
  type = bool
  default = true
}
variable "lifecycle_rule_age" {
  type = number
}
# variable "project_name" {
#   type = string
# }
variable "region" {
  type = string
}
variable "project_id" {
  type = string
}
variable "credentials" {
  type = string
}
variable "public_access_prevention" {
  type = string
  default = "enforced"
}