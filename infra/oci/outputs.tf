output "public_ip" {
  value       = oci_core_instance.vm.public_ip
  description = "Point your DNS name (for example a DuckDNS subdomain) at this address, then run the deploy workflow."
}

output "ssh" {
  value = "ssh ubuntu@${oci_core_instance.vm.public_ip}"
}
