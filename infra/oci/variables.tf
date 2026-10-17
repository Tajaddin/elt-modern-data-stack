# Account identity (from the OCI console: Profile > API keys). Put values in terraform.tfvars, which is gitignored.
variable "tenancy_ocid" { type = string }
variable "user_ocid" { type = string }
variable "fingerprint" { type = string }
variable "private_key_path" { type = string }
variable "region" {
  type    = string
  default = "us-ashburn-1"
}
variable "compartment_ocid" {
  type        = string
  description = "Compartment to create everything in (the tenancy root OCID works for a personal account)."
}

variable "ssh_public_key" {
  type        = string
  description = "Public key installed for the ubuntu user; the deploy workflow and you connect with its private half."
}

# Always Free Ampere A1 allowance (2026): up to 2 OCPUs / 12 GB in most free-tier accounts.
variable "instance_shape" {
  type    = string
  default = "VM.Standard.A1.Flex"
}
variable "ocpus" {
  type    = number
  default = 2
}
variable "memory_gb" {
  type    = number
  default = 12
}
variable "boot_volume_gb" {
  type    = number
  default = 100 # Always Free allows 200 GB of block storage in total
}

variable "allowed_ssh_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "Narrow this to your IP when you know it; the deploy workflow needs GitHub's runners to reach port 22."
}

variable "name" {
  type    = string
  default = "ghcn-elt"
}
