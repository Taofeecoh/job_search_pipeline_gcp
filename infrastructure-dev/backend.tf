terraform {
  backend "gcs" {
    bucket  = "tao-test2"
    prefix  = "terraform/dev/state"
  }
}
