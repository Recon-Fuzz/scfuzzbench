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
  description = "Worker instance IDs by slot."
  value       = { for key, instance in aws_instance.fuzzer : key => instance.id }
}

output "instance_public_ips" {
  description = "Worker public IPs by slot."
  value       = { for key, instance in aws_instance.fuzzer : key => instance.public_ip }
}

output "requested_shards" {
  description = "Total shard count requested for this run."
  value       = local.requested_shard_count
}

output "max_parallel_effective" {
  description = "Effective worker concurrency for this run."
  value       = local.max_parallel_effective
}

output "shards" {
  description = "Shard descriptors for this run."
  value = [
    for shard in local.instances : {
      shard_key  = shard.key
      fuzzer_key = shard.fuzzer_key
      run_index  = shard.run_index
    }
  ]
}

output "control_lock_object_key" {
  description = "S3 object key used for the global run lock lease."
  value       = local.control_lock_object_key
}
