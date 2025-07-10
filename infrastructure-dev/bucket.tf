# resource "random_id" "default" {
#   byte_length = 8
# }

# resource "google_storage_bucket" "default" {
#   name     = "${random_id.default.hex}-terraform-remote-backend"
#   location = "US"

#   force_destroy               = false
#   public_access_prevention    = "enforced"
#   uniform_bucket_level_access = true

#   versioning {
#     enabled = true
#   }
# }


resource "google_storage_bucket" "general_dump" {
  name = "general-dump"
  location = "us-central1"
  force_destroy = true

  public_access_prevention = "enforced"
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}