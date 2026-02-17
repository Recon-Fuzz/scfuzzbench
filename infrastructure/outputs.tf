output "bucket_name" {
  description = "S3 bucket used for logs."
  value       = local.bucket_name
}

output "run_id" {
  description = "Run identifier used in the S3 prefix."
  value       = local.run_id
}

output "benchmark_uuid" {
  description = "Benchmark UUID used for S3 prefixes."
  value       = nonsensitive(local.benchmark_uuid)
}

output "ssh_private_key_path" {
  description = "Local path to the generated SSH private key."
  value       = local_sensitive_file.ssh_private_key.filename
}

output "instance_ids" {
  description = "Worker instance IDs by worker index."
  value       = { for key, instance in aws_instance.fuzzer : key => instance.id }
}

output "instance_public_ips" {
  description = "Worker public IPs by worker index."
  value       = { for key, instance in aws_instance.fuzzer : key => instance.public_ip }
}

output "shard_count" {
  description = "Total number of queued shards in this run."
  value       = local.shard_count
}

output "max_parallel_instances" {
  description = "Configured parallel worker pool size."
  value       = var.max_parallel_instances
}

output "lock_owner" {
  description = "Owner token used for the run's global S3 lock."
  value       = local.lock_owner
}
