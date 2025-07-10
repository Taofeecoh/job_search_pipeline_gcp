resource "random_id" "my_bq_dataset" {
  byte_length = 8
}

resource "google_bigquery_dataset" "egnr_dataset" {
  dataset_id                  = "mybqdatasetid"
  friendly_name               = "mydemobqdataset"
  description                 = "This is a test description"
  location                    = "US"
  default_table_expiration_ms = 3600000

  labels = {
    env = "development"
    managed = "terraform_managed"
  }
}

resource "random_id" "my_bq_table" {
  byte_length = 8
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.egnr_dataset.dataset_id
  table_id   = "mybqtableid" # random_id.my_bq_table.hex


  labels = {
    env = "development"
    managed = "terraform_managed"
  }

}
